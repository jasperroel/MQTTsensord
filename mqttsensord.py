#!/usr/bin/env python3
import sys
import time
import argparse
import logging
import daemon
import json
import paho.mqtt.client as mqtt
import lockfile
import re
import subprocess

debug_p = False
logger = logging.getLogger(__name__)


#
# wrapper for MQTT JSON generation
#
def json_response(data):
    return json.dumps(data, sort_keys=True)


#
# apcaccess_host() get APC UPS info for given host and port
#
def apcaccess_json(host='localhost', port='3551'):
    apcaccess_cmd = '/sbin/apcaccess'
    apcaccess_host = str(host) + ":" + str(port)
    apcaccess_args = [apcaccess_cmd, '-h', apcaccess_host]
    wanted_keys = (
        'APC',
        'DATE',
        'HOSTNAME',
        'VERSION',
        'UPSNAME',
        'CABLE',
        'DRIVER',
        'UPSMODE',
        'STARTTIME',
        'MODEL',
        'STATUS',
        'LINEV',
        'LOADPCT',
        'BCHARGE',
        'TIMELEFT',
        'MBATTCHG',
        'MINTIMEL',
        'MAXTIME',
        'MAXLINEV',
        'MINLINEV',
        'OUTPUTV'
    )
    units_re = re.compile(' (Percent|Volts|Minutes|Seconds)$', re.IGNORECASE)
    errors = 0
    ups_data = {}

    # run apcaccess process to get UPS state
    try:
        apcaccess_subprocess = subprocess.Popen(apcaccess_args,
                                                stdout=subprocess.PIPE,
                                                stderr=subprocess.STDOUT)
        stdout, stderr = apcaccess_subprocess.communicate()
    except Exception as e:
        ups_data['error_msg'] = "Error parsing apcupsd line: {}".format(e)
        return json_response(ups_data)

    # check the return code
    if stderr or apcaccess_subprocess.returncode:
        ups_data['errors'] = 1
        ups_data['returncode'] = apcaccess_subprocess.returncode
        if stderr:
            ups_data['error_msg'] = stderr.decode('utf-8')
        elif stdout:
            ups_data['error_msg'] = stdout.decode('utf-8')
        else:
            ups_data['error_msg'] = "Command exited with non-zero return code"
        return json_response(ups_data)

    # parse the response
    for rawline in stdout.decode('utf-8').splitlines():
        line = rawline.rstrip()
        try:
            (k, v) = [s.rstrip() for s in line.split(': ', 1)]
            if k in wanted_keys:
                units_match = re.search(units_re, v)
                units = ''
                if units_match:
                    units = re.sub(' ', '_', units_match.group(0))
                if units != '':
                    v = re.sub(units_re, '', v)
                    k = k + units.upper()
                # Try to parse the numeric items
                try:
                    v = float(v)
                except ValueError:
                    pass
                ups_data[k] = v
        except Exception as e:
            logger.error("Error parsing apcupsd line: {}".format(e))
            logger.error(line)
            errors = errors + 1

    if errors > 0:
        ups_data["errors"] = errors

    return json_response(ups_data)


#
# read_sensor()  read an individual sensor and send MQTT message
#
def read_sensor(client, sensor):
    sensor_type = sensor['type']
    if sensor_type == 'apcups':
        sensor_data = apcaccess_json(sensor['host'], sensor['port'])
    else:
        sensor_data = json_response({'error': 'bad sensor type: ' + sensor_type})

    logger.debug("update sensor data: %s", sensor['topic'])

    now = time.time()
    sensor['last_updated'] = now

    logger.debug('Sensor Topic: %s', sensor['topic'])

    # Only send update if the results are different or we're past the
    # minimum update period
    if (sensor_data != sensor['last_sent_data']) or (sensor['last_sent_time'] + sensor['update_interval'] < now):

        if sensor['last_sent_time'] + sensor['update_interval'] < now:
            logger.debug('Update_interval exceeded')
        else:
            logger.debug('Data changed')
        logger.debug('PUBLISHING SENSOR DATA to MQTT')

        client.publish(sensor['topic'], payload=sensor_data, qos=0,
                       retain=False)

        logger.debug("publish sensor data: " + sensor['topic'])

        sensor['last_sent_data'] = sensor_data
        sensor['last_sent_time'] = now
    else:
        logger.debug('NO CHANGE in DATA')


#
# Callback for when the client receives a CONNACK response from the server.
#
# (_flags is unused)
#
def on_connect(client, userdata, _flags, rc):
    if 'subscribe' in userdata:
        for subscribe_topic in userdata['subscribe']:
            client.subscribe(subscribe_topic)
            # log result codes
            if rc != 0:
                userdata['logger'].warning("subscibing to topic [" +
                                           subscribe_topic +
                                           "] result code " + str(rc))
            else:
                userdata['logger'].debug("subscibing to topic [" +
                                         subscribe_topic +
                                         "] result code " + str(rc))
    # Send notify messages if needed
    if 'notify' in userdata:
        for notify_topic in userdata['notify']:
            client.publish(notify_topic, payload='{"notify":"true"}',
                           qos=0, retain=False)


#
# setup logging
#
def set_logging(logf):
    global debug_p
    if debug_p:
        logger.setLevel(logging.DEBUG)
        ch = logging.StreamHandler()
        ch.setLevel(logging.DEBUG)
        logger.addHandler(ch)
    else:
        logger.setLevel(logging.INFO)
    fh = logging.FileHandler(logf)
    formatstr = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    formatter = logging.Formatter(formatstr)
    fh.setFormatter(formatter)
    logger.addHandler(fh)


#
# read config file
#
def get_config_file(configf):
    with open(configf) as json_data_file:
        try:
            logger.debug("config found: %s", json_data_file)
            return json.load(json_data_file)
        except json.JSONDecodeError as parse_error:
            logger.error("JSON decode failed. [" + parse_error.msg + "]")
            logger.error("error at pos: ", parse_error.pos, " line: ", parse_error.lineno)
            sys.exit(1)


def connect_to_mqtt(host, port, client_id, username, password):
    logger.info("connecting to host " + host + ":" + str(port))

    # how to mqtt in python see https://pypi.org/project/paho-mqtt/
    mqtt_client = mqtt.Client(client_id=client_id,
                              clean_session=True)

    mqtt_client.username_pw_set(username, password)

    # create callbacks
    mqtt_client.on_connect = on_connect

    if port == 4883 or port == 4884 or port == 8883 or port == 8884:
        mqtt_client.tls_set('/etc/ssl/certs/ca-certificates.crt')

    mqtt_client.connect(host, port, 60)
    mqtt_client.loop_start()

    return mqtt_client


#
# This starts the work part of the deamon. Starts MQTT client.
# Runs scheduler loop to poll sensors.
#
def start_processing(configf):
    config_data = get_config_file(configf)

    # connect to MQTT server
    host = config_data['mqtt_host']
    port = config_data['mqtt_port'] if 'mqtt_port' in config_data else 8883
    # Client id needs to be unique to this client
    client_id = config_data['client_id'] if 'client_id' in config_data else 'mqttsensord'
    username = config_data['mqtt_user']
    password = config_data['mqtt_password']
    mqtt_client = connect_to_mqtt(host, port, client_id, username, password)

    default_interval = config_data['default_interval'] if 'default_interval' in config_data else 5

    for sensor in config_data['sensors']:
        # set defaults
        poll_interval = sensor['poll_interval'] if 'poll_interval' in sensor else default_interval
        update_interval = sensor['update_interval'] if 'update_interval' in sensor else 0
        sensor['last_sent_data'] = None
        sensor['last_sent_time'] = 0
        sensor['last_updated'] = 0
        sensor['update_interval'] = update_interval
        sensor['poll_interval'] = poll_interval

    for sensor in config_data['sensors']:
        # first time for each sensor
        read_sensor(mqtt_client, sensor)

    while True:
        time.sleep(1)
        now = time.time()
        for sensor in config_data['sensors']:
            if sensor['last_updated'] + sensor['poll_interval'] < now:
                try:
                    read_sensor(mqtt_client, sensor)
                except Exception as e:
                    logger.error("read_sensor failed: {}".format(e))
                    logger.error("read_sensor failed: {}".format(sensor))
                logger.debug(sensor)
                logger.debug('---------------------')

    # The below code is unreachable (because of "while True:")
    # mqttc.disconnect()
    # mqttc.loop_stop()


def start_daemon(pidf, logf, wdir, configf, nodaemon):
    global debug_p

    set_logging(logf)
    if nodaemon:
        # non-daemon mode, for debugging.
        logger.info('Non-Daemon mode.')
        start_processing(configf)
    else:
        # daemon mode
        logger.debug('mqttsensor: entered run()')
        logger.debug('mqttsensor: pidf = {}    logf = {}'.format(pidf, logf))
        logger.debug('mqttsensor: about to start daemonization')

        with daemon.DaemonContext(working_directory=wdir, umask=0o002, pidfile=lockfile.FileLock(pidf)):
            start_processing(configf)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='MQTT Sensor Daemon')
    parser.add_argument('-p', '--pid-file', default='mqttsensor.pid')
    parser.add_argument('-l', '--log-file', default='mqttsensor.log')
    parser.add_argument('-d', '--working-dir', default='.')
    parser.add_argument('-c', '--config-file', default='mqttsensord.json')
    parser.add_argument('-n', '--no-daemon', action="store_true")
    parser.add_argument('-v', '--verbose', action="store_true")

    args = parser.parse_args()

    if args.verbose:
        debug_p = True

    start_daemon(pidf=args.pid_file,
                 logf=args.log_file,
                 wdir=args.working_dir,
                 configf=args.config_file,
                 nodaemon=args.no_daemon)
