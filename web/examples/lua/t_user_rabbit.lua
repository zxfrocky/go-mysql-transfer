local json = require("json")   -- 加载json模块
local ops = require("mqOps") --加载mq操作模块

local row = ops.rawRow()  --当前数据库的一行数据,table类型，key为列名称
local action = ops.rawAction()  --当前数据库事件,包括：insert、updare、delete

local id = row["ID"] --获取ID列的值
local userName = row["USER_NAME"] --获取USER_NAME列的值
local password = row["PASSWORD"] --获取USER_NAME列的值
local createTime = row["CREATE_TIME"] --获取CREATE_TIME列的值

local result = {}  -- 定义一个table,作为结果
result["id"] = id
result["action"] = action

if action == "delete" -- 删除事件
then
	local val = json.encode(result) -- 将result转为json
	ops.SEND("user_topic",val) -- 发送消息，第一个参数为topic(string类型)，第二个参数为消息内容
else 
	result["userName"] = userName
	result["password"] = password
	result["createTime"] = createTime
	result["source"] = "binlog" -- 数据来源
	local val = json.encode(result) -- 将result转为json
	ops.SEND("user_topic",val) -- 发送消息，第一个参数为topic(string类型)，第二个参数为消息内容
end 