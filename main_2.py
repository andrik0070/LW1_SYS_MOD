import numpy as np
from random import uniform
from math import sqrt, log, pow
import queue
from arrival import Arrival
from pprint import pprint
import csv


class Simulation:
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

    def modeling(self):
        keep = True
        while keep:

            if self.events["first"] == float('inf'):
                self.events["first"] = self.current_time + self.interarrival_time_first_stream()

            if self.events["second"] == float('inf'):
                self.events["second"] = self.current_time + self.interarrival_time_second_stream()

            closest_event_name = min(self.events, key=self.events.get)

            self.current_time = self.events[closest_event_name]
            keep = self.current_time <= self.simulation_time

            if keep:
                if closest_event_name == "first" or closest_event_name == "second":
                    arrival = Arrival(closest_event_name, self.events[closest_event_name])
                    self.arrival(arrival)
                    self.events[closest_event_name] = float('inf')
                else:
                    self.departure()
                self.collect_statistics(closest_event_name)

        if self.current_service_time != float('inf'):
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

        pprint(self.server_util_coef())
        pprint(self.max_first_stream_arrivals_amount_in_queue)

        firstStreamItemsAverageDelay = self.delay_time_sum_first / self.total_amount_of_first_stream_arrivals
        secondStreamItemsAverageDelay = self.delay_time_sum_second / self.total_amount_of_second_stream_arrivals

        pprint(firstStreamItemsAverageDelay)
        pprint(secondStreamItemsAverageDelay)

        totalAmountOfItems = self.total_amount_of_first_stream_arrivals + self.total_amount_of_second_stream_arrivals
        averageDelay = (self.delay_time_sum_first + self.delay_time_sum_second) / totalAmountOfItems
        pprint(averageDelay)

        pprint((self.total_amount_of_first_stream_arrivals / 500) * firstStreamItemsAverageDelay)
        pprint((self.total_amount_of_second_stream_arrivals / 500) * secondStreamItemsAverageDelay)
        pprint((totalAmountOfItems / 500) * averageDelay)

        averageServiceTimeFirstStream = (
                                                self.service_time_sum_first + self.delay_time_sum_first) / self.total_amount_of_first_stream_arrivals
        averageServiceTimeSecondStream = (
                                                 self.service_time_sum_second + self.delay_time_sum_second) / self.total_amount_of_second_stream_arrivals

        pprint(averageServiceTimeFirstStream)
        pprint(averageServiceTimeSecondStream)

        pprint((
                       self.service_time_sum_first + self.service_time_sum_second + self.delay_time_sum_first + self.delay_time_sum_second) / (
                       self.total_amount_of_first_stream_arrivals + self.total_amount_of_second_stream_arrivals))

        pprint(self.total_amount_of_first_stream_arrivals)
        pprint(self.total_amount_of_second_stream_arrivals)
        pprint(self.total_amount_of_first_stream_serviced_arrivals)
        pprint(self.total_amount_of_second_stream_serviced_arrivals)
        pprint(self.max_arrivals_amount_in_queue)

        with open('statistics.csv', 'w') as csvfile:
            fieldnames = ['type', 'eventTime', 'L1', 'L2', 'departureTime', 'serverBusy', 'queueLength', 'queueContent']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames, dialect='excel')

            writer.writeheader()
            writer.writerows(self.statistics)

    def departure(self):
        self.service_time.append(self.current_service_time)

        if self.currently_serviced_arrival:
            if self.currently_serviced_arrival.type == 'first':
                self.service_time_sum_first += self.current_service_time
                self.total_amount_of_first_stream_serviced_arrivals += 1
            else:
                self.service_time_sum_second += self.current_service_time
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
            self.current_service_time = self.generate_service_time(arrival.type)
            self.events["departure"] = self.current_time + self.current_service_time
        else:
            self.events["departure"] = float('inf')

    def arrival(self, arrival):
        self.arrival_time.append(arrival.time)
        if arrival.type == "first":
            self.total_amount_of_first_stream_arrivals = self.total_amount_of_first_stream_arrivals + 1
        else:
            self.total_amount_of_second_stream_arrivals = self.total_amount_of_second_stream_arrivals + 1

        if not self.arrival_queue.qsize() and self.events["departure"] == float('inf'):
            self.current_service_time = self.generate_service_time(arrival.type)
            self.currently_serviced_arrival = arrival
            self.currently_serviced_arrival.service_start_time = arrival.time
            self.events["departure"] = self.current_time + self.current_service_time
        else:
            if self.max_arrivals_amount_in_queue < self.arrival_queue.qsize():
                self.max_arrivals_amount_in_queue += 1

            if arrival.type == "first":
                self.first_stream_arrivals_amount_in_queue += 1
                if self.first_stream_arrivals_amount_in_queue > self.max_first_stream_arrivals_amount_in_queue: self.max_first_stream_arrivals_amount_in_queue = self.first_stream_arrivals_amount_in_queue
            self.arrival_queue.put(arrival)

    def collect_statistics(self, typeOfEvent):
        event = {}
        event["type"] = typeOfEvent
        event["eventTime"] = self.current_time
        event["L1"] = self.events["first"]
        event["L2"] = self.events["second"]
        event["departureTime"] = self.events["departure"]
        event["serverBusy"] = 0 if self.events["departure"] == float('inf') else 1
        event["queueLength"] = self.arrival_queue.qsize()
        event["queueContent"] = ' '.join(list(
            map(lambda arrivalType: "L1" if ("first" == arrivalType) else "L2", list(self.arrival_queue.queue))))
        self.statistics.append(event)

    def interarrival_time_first_stream(self):
        return self.erlang(3, 4)

    def interarrival_time_second_stream(self):
        return self.exponential(0.5)

    def service_time_second_stream(self):
        return self.normal(12, 2)

    def service_time_first_stream(self):
        return self.exponential(2)

    def generate_service_time(self, arrivalType):
        return self.service_time_second_stream() if (arrivalType == "first") else self.service_time_first_stream()

    def server_util_coef(self):
        serviceTimeSum = 0
        for time in self.service_time:
            serviceTimeSum += time
        return serviceTimeSum / self.simulation_time

    def exponential(self, lam):
        return (-1 / lam) * np.log(uniform(0, 1.0))

    def erlang(self, k, lam):
        number = 1.0
        for x in range(1, k + 1):
            number = number * (1.0 - uniform(0, 1.0))
        return -lam * np.log(number)

    def normal(self, mean, std):
        x = uniform(-1, 1)
        y = uniform(-1, 1)
        s = pow(x, 2) + pow(y, 2)
        while s > 1 or s == 0:
            x = uniform(-1, 1)
            y = uniform(-1, 1)
            s = (pow(x, 2)) + (pow(y, 2))
        z = x * sqrt((-2 * log(s)) / s)
        return mean + std * z


simulation = Simulation(500)
simulation.modeling()
