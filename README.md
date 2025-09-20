```sh
{"arr":[10,20,30],"foo":1,"nested":{"bar":"baz"}}
{"arr":[100,200,300],"foo":2,"nested":{"bar":"BAZ"}}

--------
Running
--------

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

{"arr":[10,99,30],"foo":42,"nested":{"bar":"changed"}}
{"arr":[100,99,300],"foo":42,"nested":{"bar":"changed"}}
{"sum":330}
```
