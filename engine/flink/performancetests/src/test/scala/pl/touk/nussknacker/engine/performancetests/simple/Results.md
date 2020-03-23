Setup
======
50M records, running with profiling

Case1
=====
Condition: `#input.field2 % 1000000 == 0` - interpreted
Variables: `GEO, NUMERIC, DATE`
Listeners: `Logging`
Time: 93393

Case2
=====
Condition: `#input.field2() % 1000000 == 0L` - compiled
Variables: `GEO, NUMERIC, DATE`
Listeners: `Logging`
Time: 84539

Case3
=====
Condition: `#input.field2() % 1000000 == 0L` - compiled
Variables: `None`
Listeners: `None`
Time: 50324

Case3
=====
Condition: `#input.field2() % 1000000 == 0L` - compiled
Variables: Bez meta + optymalizacja
Listeners: `None`
WYŁĄCZENIE histogramu?
Time: 31051