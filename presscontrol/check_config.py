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