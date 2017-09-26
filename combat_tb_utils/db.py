import logging
import os
import socket
import sys
import time
from neomodel import db

class GraphDb(object):
    def __init__(self, host, password=None, bolt_port=7687, http_port=7474,
                 use_bolt=False, debug=False):
        if password is None:
            password = ''
        self.debug = debug
        self.logger = logging.getLogger(__name__)
        if self.debug:
            os.environ['NEOMODEL_CYPHER_DEBUG'] = '1'
            self.logger.setLevel(logging.DEBUG)
        else:
            self.logger.setLevel(logging.INFO)
        self.graph = self.connect(host=host,
                                  password=password,
                                  bolt_port=bolt_port,
                                  http_port=http_port,
                                  use_bolt=use_bolt)

    def connect(self, host='localhost', username='neo4j', password='',
                bolt_port=None, http_port=None, use_bolt=False,
                timeout=30):
        """connect - make connection to Neo4j DB

        :type host: str - hostname or IP of Neo4j database server
        :type password: str - password for Neo4j database server
        :type bolt_port: int - port for Neo4j Bolt protocol
        :type http_port: int - port for Neo4j HTTP protocol
        :type timeout: int - timeout for waiting for the Neo4j connection"""

        connected = False
        if use_bolt:
            test_port = bolt_port
        else:
            test_port = http_port
        scheme = 'http' if not use_bolt else 'bolt'
        self.logger.debug("testing if we can connect to {} which should be open for {}".format(test_port, scheme))
        while timeout > 0:
            try:
                socket.create_connection((host, test_port), 1)
            except socket.error:
                timeout -= 1
                time.sleep(1)
            else:
                connected = True
                break
        if not connected:
            raise socket.timeout('timed out trying to connect to {}'.format(
                host, test_port))
        self.bolt_port = bolt_port
        self.http_port = http_port
        self.logger.info(
            "connecting to http port: {} bolt_port: {} host: {} bolt: {}\n".
            format(http_port, bolt_port, host, use_bolt))
        # wait for server to be up for sure, needed for neo4j in Docker
        time.sleep(5)


        if password == '':
            pass_string = ':'
        else:
            pass_string = ':' + password
        port = bolt_port if use_bolt else http_port
        self.logger.debug(
            "connecting graph using {} on port: {} at host: {}".format(
                scheme, test_port, host))

        connect_url = '{scheme}://{user}{pass_string}@{host}:{port}'.format(scheme=scheme,
                                                                     user=username,
                                                                     pass_string=pass_string,
                                                                     host=host,
                                                                     port=port)
        db.set_connection(connect_url)

        self.logger.debug("connected")
        return db
