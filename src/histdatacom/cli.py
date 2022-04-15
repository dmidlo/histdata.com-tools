import argparse, sys
from histdatacom.fx_enums import Pairs, Platform, Timeframe
from histdatacom.utils import get_current_datemonth_GMTplus5

class ArgsNamespace:
    """ An intra-class DTO for Default Arguments for _HistDataCom class. """
    # argparse uses a thin class to create a namespace for cli/shell arguments to live in
    # normally argparse.ArgumentParser.parse_args(namespace=...) creates this namespace and 
    # writes user's cli args to it.  Preemptively creating here to hold default args; if the 
    # user enters args in the shell, these values will be respectively overwritten
    def __init__(self):
        self.download_data_archives = False
        self.pairs = Pairs.list_keys()
        self.platforms = Platform.list_values()
        self.timeframes = Timeframe.list_keys()
        self.start_yearmonth = "2000-00"
        self.end_yearmonth = get_current_datemonth_GMTplus5()
        self.data_directory = "data"
        self.clean_csvs = 0
        self.import_to_influxdb = 0
        self.with_all_defaults = 0

class ArgParser(argparse.ArgumentParser):
    """ Encapsulation class for argparse related operations """
    
    def __init__(self, **kwargs):
        """ set up argparse, bring in defaults DTO, setup cli params, receive 
            and overwrite defaults with user cli args."""

        # init _HistDataCom.ArgParser to extend argparse.ArgumentParser
        argparse.ArgumentParser.__init__(self)

        # bring in the defaults arg DTO from outer class, use the
        # __dict__ representation of it to set argparse argument defaults.
        self.arg_namespace = ArgsNamespace()
        self._default_args = self.arg_namespace.__dict__
        self.set_defaults(**self._default_args)

        # Nothing special here, adding cli params
        # metavar="..." is used to limit the display of choices="large iterables".
        self.add_argument(
                "-D","--download_data_archives", 
                action='store_true',
                help='download specified pairs/platforms/timeframe and create data files')
        self.add_argument(
                '-p','--pairs',
                nargs='+',
                type=str,
                choices=Pairs.list_keys(), 
                help='space separated currency pairs. e.g. -p eurusd usdjpy ...',
                metavar='PAIR')
        self.add_argument(
                '-P','--platforms',
                nargs='+',
                type=str,
                choices=Platform.list_values(), 
                help='space separated Platforms. e.g. -P metatrader ascii ninjatrader metastock',
                metavar='PLATFORM')
        self.add_argument(
                '-t','--timeframes',
                nargs='+',
                type=(lambda v : Timeframe(v).name), # convert long Timeframe .value to short .key
                choices=Timeframe.list_keys(), 
                help='space separated Timeframes. e.g. -t tick-data-quotes 1-minute-bar-quotes ...',
                metavar='TIMEFRAME')
        self.add_argument(
                "-s","--start_yearmonth", 
                type=str,
                help='add data headers to CSVs and convert EST(noDST) to UTC timestamp')
        self.add_argument(
                "-e","--end_yearmonth", 
                type=str,
                help='add data headers to CSVs and convert EST(noDST) to UTC timestamp')
        self.add_argument(
                "-C","--clean_csvs", 
                type=int,
                nargs="?",
                const=1,
                help='add data headers to CSVs and convert EST(noDST) to UTC timestamp')
        self.add_argument(
                "-I","--import_to_influxdb", 
                type=int, 
                nargs="?",
                const=1,
                help='import csv data to influxdb instance. Use influxdb.yaml to configure. Implies -C --clean_csvs')
        self.add_argument(
                '-d','--data-directory',
                type=str,
                help='Not an Executable Search Path! This directory is used to perform work. default is "data" in the current directory')

        # prevent running from cli with no arguments
        if len(sys.argv)==1:
            self.print_help(sys.stderr)
            sys.exit(1)

        # Get the args from sys.argv
        self.parse_args(namespace=self.arg_namespace)

    def __call__(self):
        """ simply return the completed args object """
        return self.arg_namespace

    @classmethod
    def _arg_list_to_set(cls, args):
        """ Utility Method to search for list objects contained in args DTO and cast them as sets """
        # This is to standardize data types. If the user specifies a parameter,
        # argparse returns a list, our defaults are sets, so . 
        for arg in args:
            if isinstance(args[arg], list): args[arg] = set(args[arg])
        return args
