#!/usr/bin/env python3

import configparser
from os import path

class App():

    def __init__(self):
        config_path = path.join(path.abspath(path.dirname(__file__)), '..', 'config.ini')
        config = configparser.ConfigParser()
        config.read(config_path)
        
        self.config = config

    def settings(self):
        return self.config 
