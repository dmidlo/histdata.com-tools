# Call Graph Visualization using gprof2dot

- https://github.com/jrfonseca/gprof2dot
```sh
$ sudo port install graphviz

$ pip install gprof2dot

$ python -m cProfile -o output.pstats src/histdatacom/histdata_com.py -X -p eurusd -f ascii -t tick-data-quotes -s 2021-01 -e now

$ gprof2dot -f pstats output.pstats | dot -Tpng -o output.png
```

# Visualization using SnakeViz

```
$ pip install snakeviz

$ python -m cProfile -o output.pstats src/histdatacom/histdata_com.py -X -p eurusd -f ascii -t tick-data-quotes -s 2021-01 -e now

$ snakeviz output.pstats
```

# Multiprocess using Vistracer

```sh
$ pip install viztracer

$ viztracer --tracer_entries 2500000 -m histdatacom -- -X -p eurusd -f ascii -t tick-data-quotes -s 2022-01 -e 2022-02

$ vizviewer /Users/davidmidlo/projects/histdata_com_tools/result.json

$ vizviewer --flamegraph /Users/davidmidlo/projects/histdata_com_tools/result.json
```

# code2flow

```sh
$ pip install code2flow

$ code2flow src/
```

# vprof

```sh
$ pip install vprof

$ vprof -c cpmh "src/histdatacom/histdata_com.py -X -p eurusd -f ascii -t tick-data-quotes -s 2022-01 -e 2022-02"
```

pycallgraph2

```sh
$ pip install pycallgraph2

$ pycallgraph graphviz -- src/histdatacom/histdata_com.py -X -p eurusd -f ascii -t tick-data-quotes -s 2022-01 -e 2022-02
``
`