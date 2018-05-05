
class Arrival:
    def __init__(self, stream_type, time):
      self.type = stream_type
      self.time = time
      self.service_start_time = None
      self.service_time = float('inf')
