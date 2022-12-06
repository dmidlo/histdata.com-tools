## Visualization using SnakeViz

```
$ pip install snakeviz

$ python -m cProfile -o output.pstats src/histdatacom/histdata_com.py -X -p eurusd -f ascii -t tick-data-quotes -s 2021-01 -e now

$ snakeviz output.pstats
```

## Multiprocess using Vistracer

```sh
$ pip install viztracer

$ viztracer --tracer_entries 2500000 -m histdatacom -- -X -p eurusd -f ascii -t tick-data-quotes -s 2022-01 -e 2022-02

$ vizviewer /Users/davidmidlo/projects/histdata_com_tools/result.json

$ vizviewer --flamegraph /Users/davidmidlo/projects/histdata_com_tools/result.json
```



## vprof

```sh
$ pip install vprof

$ vprof -c cpmh "src/histdatacom/histdata_com.py -X -p eurusd -f ascii -t tick-data-quotes -s 2022-01 -e 2022-02"
```

## gource

```sh
gource
```