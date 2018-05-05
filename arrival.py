

class Arrival:
    def __init__(self,typeOfStream, time):
      self.type = typeOfStream
      self.time = time
      self.service_start_time = None
      self.service_time = None
