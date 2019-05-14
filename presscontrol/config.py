import os
from ruamel import yaml

def read_config():
    '''Loads config.yaml. Looks for config file in this order: (1) ~/presscontrol/config.yaml, (2) ~/.config/presscontrol/config.yaml, (3) ./config.yaml.'''
    config = None
    
    for loc in [os.environ['HOME']+'/presscontrol/config.yaml',
                os.environ['HOME']+'/.config/presscontrol/config.yaml', 
                './config.yaml']:
        try:
            with open(loc) as c:
                config = yaml.load(c, Loader = yaml.RoundTripLoader)
            break
            
        except IOError:
            pass
        
    if config == None:
        config = {}

    return config, loc

config, config_loc = read_config()