# Description
Lua benchmark for evaluation large number of data access expressions under clean Lua 5.1 and Tarantool.
# Author
Shamil Gumirov <shamil@gumirov.com>
# What this means
The task is to evaluate data access expressions. Aim of this project is to benchmark 
evaluation access speed for the following storage configurations:
1. Lua tables
..1. Sequentially
..2. With Fibers (tarantool implementation of coroutines) to estimate fibers overhead
...1. With breadth traversal
...2. With depth traversal
2. Tarantool:
..1. As is
..2. With sharding module activated (3 shards were implemented here, 1 master + 2 slaves, see run.sh script to add more)
...1. With fibers to parallelize network access
....1. With breadth traversal
....2. With depth traversal
...2. Shard without fibers
..4. Use q_insert for batch inserting (on 2 slave shard instances this gives x6 increasing in speed of data fill procedure)

I did not compared coroutines with fibers although this could be interesting.

# Configuration examples
Original Lua (does not require tarantool to run):
```lua
debug=false --debug means use small data and print verbosely. True sets REPEATS option to 1.
inmem=true --false means use tarantool engine, true means use Lua tables
sharding=false --use sharding
batch=false --use q_insert to fill tables
wide=false --means execute expressions layer by layer (breadth traversal) 
deep=false  --means run fiber for each expression simultaneously (depth traversal)
```

# License
(c) by Shamil Gumirov 2016. This code was writted under contract with private company without specifying licensing model, and without
denying me from publishing code in my repo. As this is not commercial code and it's clear for me that community can benefit from 
publishing it's here.