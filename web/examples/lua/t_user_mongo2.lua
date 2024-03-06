local ops = require("mongodbOps") --加载mongodb操作模块

local row = ops.rawRow()  --当前数据库的一行数据,table类型，key为列名称
local action = ops.rawAction()  --当前数据库事件,包括：insert、update、delete

local id = row["ID"] --获取ID列的值
local userName = row["USER_NAME"] --获取USER_NAME列的值
local password = row["PASSWORD"] --获取USER_NAME列的值
local createTime = row["CREATE_TIME"] --获取CREATE_TIME列的值
local result = {}  -- 定义一个table
result["_id"] = id -- _id为MongoDB的主键标识
result["account"] = userName
result["password"] = password
result["createTime"] = createTime
result["source"] = "binlog" -- 数据来源

if action == "insert" then -- 只监听insert事件
	ops.INSERT("t_user",result) -- 新增，第一个参数为collection名称，string类型；第二个参数为要修改的数据，talbe类型
end 