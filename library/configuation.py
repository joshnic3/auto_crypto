import yaml


def read_yaml(file_path, expected=None):
    with open(file_path, 'r') as yaml_stream:
        config_dict = yaml.load(yaml_stream, Loader=yaml.SafeLoader)
    if expected is not None:
        missing = [i for i in expected if i not in config_dict]
        if missing:
            raise Exception('YAML file is missing expected items!: {}'.format(', '.join(missing)))
    return config_dict


def read_global_configs(file_path):
    return read_yaml(file_path, ['db_path', 'log_directory', 'data_sources'])


def read_script_configs(file_path):
    return read_yaml(file_path, ['name', 'variables'])

