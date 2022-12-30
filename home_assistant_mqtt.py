#! /usr/bin/env python3

import time
import sys
import string
import paho.mqtt.client as mqtt
import os
import traceback
import json

class home_assistant_mqtt():

    valid_configs = {
        "name": None,
        "unique_id": None,
        "unit_of_measurement": None,
        "state_class": "measurement",
        "icon": None,
        "state_topic": None,
        "value_template": None,
        "json_attributes_topic": None,
        "device": None
    }

    def publish(client, configs, prefix, state_topic, config_topic, publish_config):
        # enrich the config settings and prepare the individual state topics
        state_topic_configs = {}
        for k,v in configs.items():
            # enrich config
            if not "label" in v:
                v["label"] = k
            if not "object_id" in v:
                v["object_id"] = prefix
            if not "name" in v:
                v["name"] = v["object_id"] + " - " + k
            if not "state_class" in v:
                v["state_class"] = "measurement"
            if not "state_topic" in v:
                v["state_topic"] = state_topic % k
            if not "unique_id" in v:
                v["unique_id"] = prefix + "-" + k
            # assign defaults
            for kd, vd in home_assistant_mqtt.valid_configs.items():
                if (kd not in v) and (vd != None):
                    v[kd] = vd
            # assign value
            if v["value"] != None:
                if v["state_topic"] not in state_topic_configs:
                    state_topic_configs[v["state_topic"]] = []
                state_topic_configs[v["state_topic"]].append(v)
        # and publish all the states
        for k,v in state_topic_configs.items():
            if len(v) == 1:
                value = v[0].get("value", None)
            else:
                value = {}
                for i in v:
                    vv = i.get("value", None)
                    if vv != None:
                        value[i[label]] = vv
                    i["value_template"] = "{{ value_json." + i["label"] + " }}"
                if len(value) > 0:
                    value = json.dumps(value, indent=4)
                else:
                    value = None
            if value != None:
                ret,mid = client.publish(k, value)
                print("%s=%s - %s" % (k,value, ret), flush=True)
                if ret == 4:
                    client.reconnect()
                    ret,mid = client.publish(k, value)
                    print("R: %s=%s - %s" % (k,value, ret), flush=True)
        # publish config when requested
        if publish_config:
            for k,v in configs.items():
                # generate a valid final config
                cfg = {}
                for key in home_assistant_mqtt.valid_configs.keys():
                    if key in v:
                        cfg[key] = v[key]
                # render the topic
                c_topic = config_topic % v["label"]
                c_json = json.dumps(cfg, indent=4)
                # and publish it
                ret, mid = client.publish(c_topic, c_json)
                if ret == 4:
                    client.reconnect()
                    ret, mid = client.publish(c_topic, c_json)
                    print("R: %s=%s - %s" % (c_topic, c_json, ret), flush=True)
