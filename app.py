from machine import Pin
import onewire
import ds18x20
import ujson as json
import uasyncio as asyncio
import network
import mqtt_as
import ntptime


def bytes_to_hex(data, separator=""):
    return separator.join(["%02X" % b for b in data])


class Application:
    def __init__(self):
        self.data = {}
        self.config = { "ssid": "OpenWRT",
                        "password": "qwerty",
                        "ds_pin": 12, 
                        "mqtt": {
                            "topic": "esp", 
                            "qos": 1, 
                            "retain": False
                        }
        }

        try:
            with open('config.json', 'rt') as fp:
                self.config.update(json.load(fp))
        except Exception as e:
            print(e)
            print("Unable to read config.json, use default parameters")
            pass

        self.ds_sensor = ds18x20.DS18X20(
            onewire.OneWire(Pin(self.config['ds_pin']))
        )
        mqtt_con = mqtt_as.config.copy()
        mqtt_con.update(self.config['mqtt'])

        n = network.WLAN(network.STA_IF)
        mac = n.config('mac')

        mqtt_con['client_id'] = 'ESP_'+bytes_to_hex(mac)
        mqtt_con['connect_coro'] = self.on_mqtt_connect

        self.mqtt = mqtt_as.MQTTClient(mqtt_con)
        pass


    async def on_mqtt_connect(self, client):
        print("MQTT broker connected")

    async def _connect_mqtt(self):
        print("Connect to MQTT broker", self.config['mqtt']['server'])
        await self.mqtt.connect()

    async def _connect_wifi(self):
        print("Checking network connectivity")
        sta = network.WLAN(network.STA_IF)
        sta.active(True)
        if not sta.isconnected():
            print("Connecting to WIFI", self.config['ssid'])
            status = -1
            sta.connect(
                self.config['ssid'], self.config['password']
            )
            while not sta.isconnected():
                if sta.status() != status:
                    status = sta.status()
                    print("status:", status)
                await asyncio.sleep(0.5)
        print("Connected to network", sta.ifconfig())

    async def _send_data(self):
        print("Start sending sensors data")
        while True:
            old_data = self.data.copy()
            await asyncio.sleep(10)
            for address, value in self.data.items():
                if address not in old_data or old_data[address] != value:
                    print("data changed", address, value)

                    await self.mqtt.publish(
                        '{}/{}'.format(self.config['mqtt']['topic'], address), 
                        '{}'.format(value), 
                        qos = self.config['mqtt']['qos'],
                        retain = self.config['mqtt']['retain']
                    )
        pass

    async def _read_data(self):
        print("Start retrieve data from sensors")
        roms = self.ds_sensor.scan()
        print("Found DS sensors:", len(roms))
        while True:
            if len(roms) > 0:
                self.ds_sensor.convert_temp()
                await asyncio.sleep_ms(750)
                for rom in roms:
                    value = self.ds_sensor.read_temp(rom)
                    address = bytes_to_hex(rom)
                    self.data[address] = value
                    print('sensor', address, value)

            await asyncio.sleep(15)

    async def run(self):
        loop = asyncio.get_event_loop()
        loop.create_task(self._read_data())

        while True:
            try:
                await self._connect_wifi()
                ntptime.settime()
                await self._connect_mqtt()
                await self._send_data()
            except Exception as e:
                print("Unhandled exception", e)
                print("Restarting after timeout")
                await asyncio.sleep(20)
        pass

print()
print("Start application")
app = Application()
loop = asyncio.get_event_loop()
loop.run_until_complete(app.run())
