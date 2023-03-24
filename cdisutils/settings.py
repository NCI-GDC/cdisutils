import yaml, pprint, os, logging, json

class Settings:

    """
    This class is used to contain the global settings.  The
    settings will be imported on the load of the module from the
    default path.

    """

    settings = {}

    default_path = "settings.yaml"

    def lookup(self, key):
        """
        Insert any indirect lookups in this function
        """

        if key not in self.settings:
            logging.error(f"Key [{key}] was not in settings dictionary")
            return None

        return self.settings[attribute]

    def __init__(self, path = None):
        self.path = self.default_path
        self.load(path)

    def __call__(self, key):
        return self.lookup(key)

    def __getitem__(self, key):
        return self.lookup(key)

    def __setitem__(self, key, value):
        self.settings[attribute] = value
        return self

    def __repr__(self):
        return str(self.settings)

    def load(self, path = None):

        if path is None and self.path is None:
            logging.error("Unable to load settings, no path specified.")
            return self

        if path is not None:
            logging.debug(f"Updating settings file path {path}")
            self.path = path

        logging.info(f"Loading settings file {path}")

        try:
            with open(self.path) as yaml_file:
                self.settings = yaml.load(yaml_file)
        except Exception as msg:
            logging.error(f"Unable to load settings from {path}: {str(msg)}")
            logging.info("Proceeding with no settings")
        else:
            logging.debug(f"Successfully loaded settings from {path}.")

        return self
