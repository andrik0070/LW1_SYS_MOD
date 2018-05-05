import numpy as np
from random import uniform
from math import sqrt, log, pow
import queue
from arrival import Arrival
import csv


class Model:
    def __init__(self, simulation_time):

        self.current_time = 0.0
        self.simulation_time = simulation_time

        self.arrival_queue = queue.Queue()
        self.arrival_time = []

        self.total_amount_of_first_stream_arrivals = 0
        self.total_amount_of_second_stream_arrivals = 0

        self.total_amount_of_first_stream_serviced_arrivals = 0
        self.total_amount_of_second_stream_serviced_arrivals = 0

        self.service_time = []

        self.service_time_sum_first = 0
        self.service_time_sum_second = 0

        self.current_service_time = None
        self.currently_serviced_arrival = None

        self.statistics = []

        self.delay_time_sum_first = 0
        self.delay_time_sum_second = 0

        self.events = {"first": self.current_time + self.interarrival_time_first_stream(),
                       "second": self.current_time + self.interarrival_time_second_stream(),
                       "departure": float('inf')}

        self.first_stream_arrivals_amount_in_queue = 0
        self.max_first_stream_arrivals_amount_in_queue = 0
        self.max_arrivals_amount_in_queue = 0

        self.collect_statistics("start")

    def simulate(self):
        while True:

            if self.events["first"] == float('inf'):
                self.events["first"] = self.current_time + self.interarrival_time_first_stream()

            if self.events["second"] == float('inf'):
                self.events["second"] = self.current_time + self.interarrival_time_second_stream()

            closest_event_name = min(self.events, key=self.events.get)

            self.current_time = self.events[closest_event_name]

            if self.current_time > self.simulation_time:
                break

            if closest_event_name == "first" or closest_event_name == "second":
                arrival = Arrival(closest_event_name, self.events[closest_event_name])
                self.arrival(arrival)
                self.events[closest_event_name] = float('inf')
            else:
                self.departure()
                self.collect_statistics(closest_event_name)

        if self.currently_serviced_arrival and self.currently_serviced_arrival.service_time != float('inf'):
            if self.currently_serviced_arrival.type == 'first':
                self.service_time_sum_first += self.simulation_time - self.currently_serviced_arrival.service_start_time
            else:
                self.service_time_sum_second += self.simulation_time - self.currently_serviced_arrival.service_start_time

        while self.arrival_queue.qsize():
            arrival = self.arrival_queue.get()
            if arrival.type == 'first':
                self.delay_time_sum_first += self.simulation_time - arrival.time
            else:
                self.delay_time_sum_second += self.simulation_time - arrival.time

        print("Коэффициент простоя сервера: " + str(1 - self.server_util_coef()))

        first_stream_arrival_average_delay = self.delay_time_sum_first / self.total_amount_of_first_stream_arrivals
        second_stream_arrival_average_delay = self.delay_time_sum_second / self.total_amount_of_second_stream_arrivals

        total_amount_of_arrivals = self.total_amount_of_first_stream_arrivals + self.total_amount_of_second_stream_arrivals
        average_delay = (self.delay_time_sum_first + self.delay_time_sum_second) / total_amount_of_arrivals

        print("Среднее число заявок первого типа в очереди: " + str((self.total_amount_of_first_stream_arrivals / 500) * first_stream_arrival_average_delay))
        print("Среднее число заявок второго типа в очереди: " + str((self.total_amount_of_second_stream_arrivals / 500) * second_stream_arrival_average_delay))
        print("Среднее число заявок в очереди независимо от типа: " + str((total_amount_of_arrivals / 500) * average_delay))

        average_service_time_first_stream = (self.service_time_sum_first + self.delay_time_sum_first) / self.total_amount_of_first_stream_arrivals
        average_service_time_second_stream = (self.service_time_sum_second + self.delay_time_sum_second) / self.total_amount_of_second_stream_arrivals

        print("Среднее время обслуживания заявок первого типа: " + str(average_service_time_first_stream))
        print("Среднее время обслуживания заявок второго  типа: " + str(average_service_time_second_stream))
        print("Среднее время обслуживания заявок независимо от типа: " + str((self.service_time_sum_first + self.service_time_sum_second + self.delay_time_sum_first + self.delay_time_sum_second) / (self.total_amount_of_first_stream_arrivals + self.total_amount_of_second_stream_arrivals)))

        print("Среднее время задержки  в очереди для заявок первого типа: " + str(first_stream_arrival_average_delay))
        print("Среднее время задержки  в очереди для заявок второго типа: " + str(second_stream_arrival_average_delay))
        print("Среднее время задержки заявок в очереди независимо от типа: " + str(average_delay))

        print("Общее кол-во сгенерированных заяавок первого типа :" + str(self.total_amount_of_first_stream_arrivals))
        print("Общее кол-во сгенерированных заяавок второго типа :" + str(self.total_amount_of_second_stream_arrivals))

        print("Общее кол-во обслуженных заяавок первого типа :" + str(self.total_amount_of_first_stream_serviced_arrivals))
        print("Общее кол-во обслуженных заяавок второго типа :" + str(self.total_amount_of_second_stream_serviced_arrivals))

        print("Максимальное кол-во заявок в очереди: " + str(self.max_arrivals_amount_in_queue))

        with open('event_calendar.csv', 'w') as file:
            fieldnames = ['type', 'time', 'L1', 'L2', 'departure_time', 'server_status', 'q_length', 'q_content']
            writer = csv.DictWriter(file, fieldnames=fieldnames, dialect='excel')

            writer.writeheader()
            writer.writerows(self.statistics)

    def collect_statistics(self, event_type):
        event = {}
        event["type"] = event_type
        event["time"] = self.current_time
        event["L1"] = self.events["first"]
        event["L2"] = self.events["second"]
        event["departure_time"] = self.events["departure"]
        event["server_status"] = 0 if self.events["departure"] == float('inf') else 1
        event["q_length"] = self.arrival_queue.qsize()
        event["q_content"] = ' '.join(list(
            map(lambda arrival_type: "L1" if ("first" == arrival_type) else "L2", list(self.arrival_queue.queue))))
        self.statistics.append(event)

    def arrival(self, arrival):
        self.arrival_time.append(arrival.time)
        if arrival.type == "first":
            self.total_amount_of_first_stream_arrivals = self.total_amount_of_first_stream_arrivals + 1
        else:
            self.total_amount_of_second_stream_arrivals = self.total_amount_of_second_stream_arrivals + 1

        if not self.arrival_queue.qsize() and self.events["departure"] == float('inf'):
            self.currently_serviced_arrival = arrival
            self.currently_serviced_arrival.service_time = self.generate_service_time(arrival.type)
            self.currently_serviced_arrival.service_start_time = arrival.time
            self.events["departure"] = self.current_time + self.currently_serviced_arrival.service_time
        else:
            if self.max_arrivals_amount_in_queue < self.arrival_queue.qsize():
                self.max_arrivals_amount_in_queue += 1
            self.arrival_queue.put(arrival)

    def departure(self):
        self.service_time.append(self.currently_serviced_arrival.service_time)

        if self.currently_serviced_arrival:
            if self.currently_serviced_arrival.type == 'first':
                self.service_time_sum_first += self.currently_serviced_arrival.service_time
                self.total_amount_of_first_stream_serviced_arrivals += 1
            else:
                self.service_time_sum_second += self.currently_serviced_arrival.service_time
                self.total_amount_of_second_stream_serviced_arrivals += 1

        if self.arrival_queue.qsize():
            arrival = self.arrival_queue.get()
            arrival.service_start_time = self.current_time
            self.currently_serviced_arrival = arrival
            if arrival.type == "first":
                self.delay_time_sum_first += self.current_time - arrival.time
                self.first_stream_arrivals_amount_in_queue -= 1
            else:
                self.delay_time_sum_second += self.current_time - arrival.time
            self.currently_serviced_arrival.service_time = self.generate_service_time(arrival.type)
            self.events["departure"] = self.current_time + self.currently_serviced_arrival.service_time
        else:
            self.events["departure"] = float('inf')
            self.currently_serviced_arrival = None

    def service_time_second_stream(self):
        return __class__.normal(12, 2)

    def service_time_first_stream(self):
        return __class__.exponential(2)

    def interarrival_time_first_stream(self):
        return __class__.erlang(3, 4)

    def interarrival_time_second_stream(self):
        return __class__.exponential(0.5)

    def server_util_coef(self):
        service_time_total = self.service_time_sum_first + self.service_time_sum_second
        return service_time_total / self.simulation_time

    def generate_service_time(self, arrival_type):
        return self.service_time_second_stream() if (arrival_type == "first") else self.service_time_first_stream()

    @staticmethod
    def exponential(lam):
        return (-1 / lam) * np.log(uniform(0, 1.0))

    @staticmethod
    def erlang(k, lam):
        y = 1.0
        for x in range(1, k + 1):
            y = y * (1.0 - uniform(0, 1.0))
        return -lam * np.log(y)

    @staticmethod
    def normal(mean, std):
        x1 = uniform(-1, 1)
        x2 = uniform(-1, 1)
        s = pow(x1, 2) + pow(x2, 2)
        while s > 1 or s == 0:
            x1 = uniform(-1, 1)
            x2 = uniform(-1, 1)
            s = (pow(x1, 2)) + (pow(x2, 2))
        z = x1 * sqrt((-2 * log(s)) / s)
        return mean + std * z


model = Model(500)
model.simulate()
