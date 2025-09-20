use std::cell::{Ref, RefCell, RefMut};
use std::rc::Rc;

use mlua::{
    Function as LuaFunction, Lua, MetaMethod, Result, UserData, UserDataMethods, Value as LuaValue,
};
use serde_json::{Value, json};

#[derive(Clone)]
struct SharedValue {
    root: Rc<RefCell<Value>>,
    path: Vec<PathElement>,
}

#[derive(Clone)]
enum PathElement {
    Key(String),
    Index(usize),
}

impl SharedValue {
    fn new(root: Value) -> Self {
        Self {
            root: Rc::new(RefCell::new(root)),
            path: Vec::new(),
        }
    }

    fn take(self) -> Value {
        let mut node = self.root.take();
        for elem in &self.path {
            node = match elem {
                PathElement::Key(k) => remove_by_key(node, k).unwrap(),
                PathElement::Index(i) => remove_by_index(node, *i).unwrap(),
            };
        }
        node
    }

    fn resolve(&self) -> Ref<'_, Value> {
        let mut node = self.root.borrow();
        for elem in &self.path {
            node = match elem {
                PathElement::Key(k) => Ref::filter_map(node, |n| n.get(k)).unwrap(),
                PathElement::Index(i) => Ref::filter_map(node, |n| n.get(*i)).unwrap(),
            };
        }
        node
    }

    fn resolve_mut(&self) -> RefMut<'_, Value> {
        let mut node = self.root.borrow_mut();
        for elem in &self.path {
            node = match elem {
                PathElement::Key(k) => RefMut::filter_map(node, |n| n.get_mut(k)).unwrap(),
                PathElement::Index(i) => RefMut::filter_map(node, |n| n.get_mut(*i)).unwrap(),
            };
        }
        node
    }

    fn subhandle(&self, elem: PathElement) -> Self {
        let mut new_path = self.path.clone();
        new_path.push(elem);
        Self {
            root: self.root.clone(),
            path: new_path,
        }
    }
}

fn remove_by_key(value: Value, key: &str) -> Option<Value> {
    if let Value::Object(mut map) = value {
        map.remove(key)
    } else {
        None
    }
}

fn remove_by_index(value: Value, index: usize) -> Option<Value> {
    match value {
        Value::Array(mut arr) if index < arr.len() => Some(arr.remove(index)),
        _ => None,
    }
}

impl UserData for SharedValue {
    fn add_methods<M: UserDataMethods<Self>>(methods: &mut M) {
        methods.add_meta_method(MetaMethod::Index, |lua, this, key: LuaValue| {
            let val = this.resolve();
            match key {
                LuaValue::String(s) => {
                    let k = s.to_str()?.to_string();
                    if let Some(child) = val.get(&k) {
                        Ok(json_subhandle_to_lua(
                            lua,
                            this.clone(),
                            child,
                            PathElement::Key(k),
                        )?)
                    } else {
                        Ok(LuaValue::Nil)
                    }
                }
                LuaValue::Integer(i) => {
                    let idx = (i - 1) as usize;
                    if let Some(child) = val.get(idx) {
                        Ok(json_subhandle_to_lua(
                            lua,
                            this.clone(),
                            child,
                            PathElement::Index(idx),
                        )?)
                    } else {
                        Ok(LuaValue::Nil)
                    }
                }
                _ => Ok(LuaValue::Nil),
            }
        });

        methods.add_meta_method_mut(
            MetaMethod::NewIndex,
            |_, this, (key, val): (LuaValue, LuaValue)| {
                let mut node = this.resolve_mut();
                let new_val = lua_to_json(val)?;
                match key {
                    LuaValue::String(s) => {
                        let key_str = s.to_str()?.to_string();
                        node[key_str] = new_val;
                    }
                    LuaValue::Integer(i) => {
                        let idx = (i - 1) as usize;
                        if let Value::Array(arr) = &mut *node
                            && idx < arr.len()
                        {
                            arr[idx] = new_val;
                        }
                    }
                    _ => {}
                }
                Ok(())
            },
        );

        methods.add_method("__pairs_impl", |lua, this, ()| {
            let this = this.clone();
            let val = this.resolve().clone();

            match val {
                Value::Object(obj) => make_iter(lua, obj, move |lua, (k, v)| {
                    Ok((
                        LuaValue::String(lua.create_string(&k)?),
                        json_subhandle_to_lua(lua, this.clone(), &v, PathElement::Key(k))?,
                    ))
                }),
                Value::Array(arr) => {
                    make_iter(lua, arr.into_iter().enumerate(), move |lua, (i, v)| {
                        Ok((
                            LuaValue::Integer(i as i64 + 1),
                            json_subhandle_to_lua(lua, this.clone(), &v, PathElement::Index(i))?,
                        ))
                    })
                }
                _ => make_iter(lua, std::iter::empty::<()>(), |_, _| {
                    Ok((LuaValue::Nil, LuaValue::Nil))
                }),
            }
        });
    }
}

fn json_subhandle_to_lua(
    lua: &Lua,
    parent: SharedValue,
    val: &Value,
    elem: PathElement,
) -> Result<LuaValue> {
    Ok(match val {
        Value::Null => LuaValue::Nil,
        Value::Bool(b) => LuaValue::Boolean(*b),
        Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                LuaValue::Integer(i)
            } else {
                LuaValue::Number(n.as_f64().unwrap())
            }
        }
        Value::String(s) => LuaValue::String(lua.create_string(s)?),
        Value::Array(_) | Value::Object(_) => {
            LuaValue::UserData(lua.create_userdata(parent.subhandle(elem))?)
        }
    })
}

fn json_to_lua(lua: &Lua, val: Value) -> Result<LuaValue> {
    Ok(match val {
        Value::Null => LuaValue::Nil,
        Value::Bool(b) => LuaValue::Boolean(b),
        Value::Number(n) => n
            .as_i64()
            .map(LuaValue::Integer)
            .unwrap_or(LuaValue::Number(n.as_f64().unwrap())),
        Value::String(s) => LuaValue::String(lua.create_string(s)?),
        Value::Array(_) | Value::Object(_) => {
            LuaValue::UserData(lua.create_userdata(SharedValue::new(val))?)
        }
    })
}

fn lua_to_json(val: LuaValue) -> Result<Value> {
    Ok(match val {
        LuaValue::Nil => Value::Null,
        LuaValue::Boolean(b) => Value::Bool(b),
        LuaValue::Integer(i) => Value::Number(i.into()),
        LuaValue::Number(f) => serde_json::Number::from_f64(f)
            .map(Value::Number)
            .unwrap_or(Value::Null),
        LuaValue::String(s) => Value::String(s.to_str()?.to_string()),
        LuaValue::Table(t) => {
            let mut arr: Vec<Value> = Vec::new();
            let mut map: serde_json::Map<String, Value> = serde_json::Map::new();
            let mut is_array = true;

            for pair in t.pairs::<LuaValue, LuaValue>() {
                let (k, v) = pair?;
                let value = lua_to_json(v)?;
                match k {
                    LuaValue::Integer(i) if i > 0 => {
                        let idx = (i - 1) as usize;
                        if idx != arr.len() {
                            is_array = false;
                        }
                        if is_array {
                            arr.push(value);
                        } else {
                            map.insert(i.to_string(), value);
                        }
                    }
                    LuaValue::String(s) => {
                        is_array = false;
                        map.insert(s.to_str()?.to_string(), value);
                    }
                    _ => {
                        is_array = false;
                    }
                }
            }

            if is_array {
                Value::Array(arr)
            } else {
                if !arr.is_empty() {
                    for (i, v) in arr.into_iter().enumerate() {
                        map.insert((i + 1).to_string(), v);
                    }
                }
                Value::Object(map)
            }
        }
        _ => Value::Null,
    })
}

fn make_iter<I, F>(lua: &Lua, iter: I, mut f: F) -> Result<(LuaFunction, LuaValue, LuaValue)>
where
    I: IntoIterator + 'static,
    F: FnMut(&Lua, I::Item) -> Result<(LuaValue, LuaValue)> + 'static,
{
    let mut it = iter.into_iter();
    let iter_fn = lua.create_function_mut(move |lua, _: ()| {
        if let Some(item) = it.next() {
            f(lua, item)
        } else {
            Ok((LuaValue::Nil, LuaValue::Nil))
        }
    })?;
    Ok((iter_fn, LuaValue::Nil, LuaValue::Nil))
}

fn run<I>(script: &str, input: I) -> Result<Vec<Value>>
where
    I: IntoIterator<Item = Value> + 'static,
{
    let lua = Lua::new();
    let input_iter = Rc::new(RefCell::new(input.into_iter()));
    let output: Rc<RefCell<Vec<Value>>> = Rc::new(RefCell::new(Vec::new()));

    {
        let input_iter = input_iter.clone();
        lua.globals().set(
            "get_next",
            lua.create_function(move |lua, ()| {
                input_iter
                    .borrow_mut()
                    .next()
                    .map_or(Ok(LuaValue::Nil), |v| json_to_lua(lua, v))
            })?,
        )?;
    }

    {
        let output = output.clone();
        lua.globals().set(
            "emit_clone",
            lua.create_function(move |_, val: LuaValue| {
                let json_val = match val {
                    LuaValue::UserData(data) => data
                        .borrow::<SharedValue>()
                        .map_or(Value::Null, |v| v.resolve().clone()),
                    _ => lua_to_json(val)?,
                };
                output.borrow_mut().push(json_val);
                Ok(())
            })?,
        )?;
    }

    {
        let output = output.clone();
        lua.globals().set(
            "emit",
            lua.create_function(move |_, val: LuaValue| {
                let json_val = match val {
                    LuaValue::UserData(data) => data
                        .borrow::<SharedValue>()
                        .map_or(Value::Null, |v| v.clone().take()),
                    _ => lua_to_json(val)?,
                };
                output.borrow_mut().push(json_val);
                Ok(())
            })?,
        )?;
    }

    lua.load(
        r#"
        local original_pairs = pairs
        function pairs(t)
            if type(t) == "userdata" and t.__pairs_impl then
                local ok, iter, state, key = pcall(function() return t:__pairs_impl() end)
                if ok then return iter, state, key end
            end
            return original_pairs(t)
        end
        "#,
    )
    .exec()?;

    println!("\n--------\nRunning\n--------\n{script}");
    lua.load(script).exec()?;
    drop(lua);

    Ok(Rc::try_unwrap(output)
        .expect("to be the last owner of the iterator")
        .into_inner())
}

fn main() -> Result<()> {
    let input = vec![
        json!({
            "foo": 1,
            "nested": { "bar": "baz" },
            "arr": [10, 20, 30]
        }),
        json!({
            "foo": 2,
            "nested": { "bar": "BAZ" },
            "arr": [100, 200, 300]
        }),
    ];
    for x in &input {
        println!("{x}");
    }

    let out = run(
        r#"
            sum = 0
            while true do
                local doc = get_next()
                if doc == nil then
                    break
                end

                doc.foo = 42
                doc.nested.bar = "changed"
                doc.arr[2] = 99

                sum = sum + doc.arr[3]

                emit(doc)
            end

            emit({sum=sum})
        "#,
        input,
    )?;

    for x in out {
        println!("{x}");
    }
    Ok(())
}
