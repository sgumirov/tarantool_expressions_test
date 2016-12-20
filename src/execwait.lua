fiber = require('fiber')
gvar = 0

function execute_and_wait(num, run_func)
  local condvar = fiber.cond()
  gvar = num
  for i=1,num,1 do
    fiber.create(function (condvar, work_func)
        work_func()
        gvar = gvar - 1
        condvar:signal()
      end, condvar, run_func
    )
  end
  while gvar > 0 do
    condvar:wait(1)
    --print("MAIN woken up gvar="..gvar)
  end
  --print("MAIN all finished SUCCESS")
end

--example of usage:
local function main()
  execute_and_wait(10000, function() fiber.sleep(math.random(10)) end)
end

main()
