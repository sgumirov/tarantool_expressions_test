killall tarantool
cd shards/
tarantool sh-test-1.lua &
tarantool sh-test-2.lua &
cd ..
cd src
tarantool main.lua
killall tarantool
cd ..
