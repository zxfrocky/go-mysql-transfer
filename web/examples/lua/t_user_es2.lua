local ops = require("esOps") --加载elasticsearch操作模块

local row = ops.rawRow()  --当前数据库的一行数据,table类型，key为列名称
local action = ops.rawAction()  --当前数据库事件,包括：insert、update、delete

local id = row["ID"] --获取ID列的值
local userName = row["USER_NAME"] --获取USER_NAME列的值
local password = row["PASSWORD"] --获取USER_NAME列的值
local createTime = row["CREATE_TIME"] --获取CREATE_TIME列的值
local remark = row["REMARK"] --获取REMARK列的值

local result = {}  -- 定义一个table,作为结果集
result["id"] = id
result["userName"] = userName
result["password"] = password
result["createTime"] = createTime
result["remark"] = remark
result["source"] = "binlog" -- 数据来源

if action == "insert" then -- 只监听新增事件
	ops.INSERT("t_user",id,result) -- 新增，参数1为index名称，string类型；参数2为要插入的数据主键；参数3为要插入的数据，tablele类型或者json字符串
end 