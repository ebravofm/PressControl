from presscontrol.config import config, config_loc
from ruamel import yaml
import os
        
def update_mysql_config():
    try:
        print('Please enter the following (Ctrl-C to pospone):\n')
        for key in [k for k in config['MYSQL'].keys()]:
            value = input(f'Enter {key} value (current: {config["MYSQL"][key]}): ')
            if value != '':
                config["MYSQL"][key] = value

        with open(config_loc, 'w') as f:
            yaml.dump(config, f, Dumper = yaml.RoundTripDumper, default_flow_style = False)
            
    except KeyboardInterrupt:
        pass


def update_config(titles=[]):
    try:
        if not titles:
            titles = [t for t in config.keys()]
            
        print('Please enter the following (Ctrl-C to pospone):')
        
        for title in titles:
            print('\n\n'+title)
            
            if isinstance(config[title], dict):
                for key in [k for k in config[title].keys()]:
                    value = parse_input(f'Enter {key} value (current: {config[title][key]}): ')
                    if value != '':
                        config[title][key] = value
                        
            if isinstance(config[title], list):
                value = parse_input(f'Enter values (comma separated) (current: {", ".join(config[title])}): ')
                if value != '':
                    config[title] = value

        with open('new_config.yaml', 'w') as f:
            yaml.dump(config, f, Dumper = yaml.RoundTripDumper, default_flow_style = False)
            
    except KeyboardInterrupt:
        pass
    
    
def parse_input(text):
    s = input(text)
    if s in ['true', 'True', 'false', 'False']:
        return bool(s)
    if ',' in s:
        return [item.strip() for item in s.split(',')]
    return s
        
    
    
def update_tables_config():
    print('Tables Config:\n')
    
    for key in [k for k in config['TABLES'].keys()]:
        
        value = input(f'Enter {key} TABLE value (current: {config["TABLES"][key]}): ')
        if value != '':
            config["TABLES"][key] = value
 
    with open('data.yaml', 'w') as f:
        yaml.dump(config, f, Dumper = yaml.RoundTripDumper, default_flow_style = False)


if None in [v for v in config['MYSQL'].values()]:
    os.system('clear')
    print('='*50, sep='')
    print('INITIALIZING MYSQL CONFIG.'.center(50,' '))
    print('='*50, '\n')
    update_mysql_config()