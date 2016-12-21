fiber = require('fiber')
gvar = 0

function execute_and_wait(list, run_func)
  local condvar = fiber.cond()
  gvar = table.getn(list)
  for k,v in pairs(list) do
    fiber.create(function (condvar, work_func, arg)
        work_func(arg)
        gvar = gvar - 1
        condvar:signal()
      end, condvar, run_func, v
    )
  end
  while gvar > 0 do
    condvar:wait(1)
  end
end

--example of usage
local function main()
  local list = {}
  for i=1,10,1 do
    list[i] = i
  end
  execute_and_wait(list, function(a) 
    print(a)
    fiber.sleep(math.random(10)) 
  end)
end
