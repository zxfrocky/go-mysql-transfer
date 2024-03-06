local ops = require("mqOps") --加载mq操作模块

local row = ops.rawRow()  --当前数据库的一行数据,table类型，key为列名称
local action = ops.rawAction()  --当前数据库事件,包括：insert、updare、delete

local userName = row["USER_NAME"] --获取USER_NAME列的值

if action == "insert" then -- 只监听添加事件
    local str = string.format("恭喜您：%s  注册成功",userName)  
    ops.SEND("user_topic",str) -- 发送消息，第一个参数为topic(string类型)，第二个参数为消息内容
end 