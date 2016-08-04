import requests, json

mesos_url = "http://localhost:5050/api/v1/scheduler"
header = {"Content-type":"application/json"}

class Framework:
    def __init__(self, name):
        self.name = name
        self.id = None
        self.subscribed = False

    def run(self):
        subscribe_data = {"type": "SUBSCRIBE","subscribe":{"framework_info": {"user": "jvanz","name": self.name},"force": True}}
        r = requests.post(mesos_url, headers=header, json=subscribe_data, stream=True)
        header["Mesos-Stream-Id"] = r.headers["Mesos-Stream-Id"]
        for chunk in r.iter_content(None):
            response = chunk.decode("utf-8").splitlines()[1]
            self.process(json.loads(response))

    def process(self,event):
        etype = event["type"]
        print("event: ", event)
        payload = { "framework_id" : {"value":self.id}}
        if "SUBSCRIBED" == etype:
            self.id = event["subscribed"]["framework_id"]["value"]
            self.subscribed= True
        elif "HEARTBEAT" == etype:
            pass
        elif "OFFERS" == etype:
            payload["type"] = "DECLINE"
            payload["decline"] = { "offer_ids" : []}
            for offer in event["offers"]["offers"]:
                payload["decline"]["offer_ids"].append({ "value": offer["id"]["value"]})
            response = requests.post(mesos_url, headers=header, json=payload)
            if response.status_code != 202:
                print("Can not decline offers")
            else:
                print("The offers have been declined")

def main():
    framework = Framework("Dummy HTTP framework")
    framework.run()

if __name__ == "__main__":
    main()
