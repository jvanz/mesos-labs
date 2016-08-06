"""
    *** This is a dummy framework ***
    This framework is implemented only for test a change in the Apache Mesos.
"""

import requests, json

mesos_url = "http://localhost:5050/api/v1/scheduler"
header = {"Content-type":"application/json"}

class Framework:
    def __init__(self, name):
        self.name = name
        self.id = None
        self.subscribed = False
        self.task_counter = 0

    def run(self):
        subscribe_data = {"type": "SUBSCRIBE","subscribe":{"framework_info": {"user": "jvanz","name": self.name},"force": True}}
        r = requests.post(mesos_url, headers=header, json=subscribe_data, stream=True)
        header["Mesos-Stream-Id"] = r.headers["Mesos-Stream-Id"]
        for chunk in r.iter_content(None):
            response = chunk.decode("utf-8").splitlines()[1]
            self.process(json.loads(response))

    def process(self,event):
        etype = event["type"]
        print("event: ", etype)
        if "SUBSCRIBED" == etype:
            self.id = event["subscribed"]["framework_id"]["value"]
            self.subscribed= True
        elif "OFFERS" == etype:
            self.launch_task(event)
        elif "UPDATE" == etype:
            self.ack(event)

    def launch_task(self, event):
        accept = {"offer_ids": [], "operations": []}
        for offer in event["offers"]["offers"]:
            accept["offer_ids"].append({"value":offer["id"]["value"]})
            mem_offer = offer["resources"][1]
            cpu = {"name": "cpus", "type": "SCALAR", "scalar":{"value": 0.5}}
            mem = {"name": "mem", "type": "SCALAR", "scalar": {"value": 50}}
            task = {"name": "task-" + str(self.task_counter), "task_id": {"value":str(self.task_counter)},
                    "agent_id": {"value":offer["agent_id"]["value"]}, "resources": [cpu,mem],
                    "command": {"value": "sleep 30", "shell": True}}
            accept["operations"].append({"type": "LAUNCH", "launch": {"task_infos": [task]}})
            self.task_counter += 1
        payload = {"type": "ACCEPT", "framework_id": {"value":self.id }, "accept": accept}
        response = requests.post(mesos_url, headers=header, json=payload)
        if response.status_code == 202:
            print("Task launched successfully")
        else:
            print("Task launch failed")

    def ack(self, event):
        payload = {"type": "ACKNOWLEDGE", "framework_id": {"value":self.id },
                "acknowledge": {
                    "agent_id": event["update"]["status"]["agent_id"],
                    "task_id": event["update"]["status"]["task_id"],
                    "uuid": event["update"]["status"]["uuid"]
                    }}
        response = requests.post(mesos_url, headers=header, json=payload)
        if response.status_code == 202:
            print("ACK OK")
        else:
            print("ACK ** NOT ** OK")


def main():
    framework = Framework("Dummy HTTP framework")
    framework.run()

if __name__ == "__main__":
    main()
