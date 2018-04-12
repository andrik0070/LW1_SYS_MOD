import sys
import numpy as np
import queue
from pprint import pprint
from distributions import *
import csv


class Simulation:
    def __init__(self, modelingTime):
        self.currentTime = 0.0
        self.modelingTime = modelingTime
        self.arrivalStack = []
        self.itemArrivalTime = []
        self.itemServiceTime = []
        self.serviceTime = None
        self.events = {"firstStream": self.currentTime + self.generateFirstStreamInterarrivalTime(),
                       "secondStream": self.currentTime + self.generateSecondStreamInterarrivalTime(),
                       "departure": float('inf')}
        self.statistics = []
        self.l1AmountInQueue = 0
        self.maxl1AmountInQueue = 0
        self.updateStatistics("start")

    def modeling(self):
        shouldRepeat = True
        while (shouldRepeat):

            # firstStreamItem = np.random.exponential(scale=5)
            # secondStreamItem =  np.random.gamma(shape=2, scale=0.1)

            # self.firstStreamQueue.appendleft(self.currentTime + firstStreamItem)
            # self.secondStreamQueue.appendleft(self.currentTime + secondStreamItem)

            if self.events["firstStream"] == float('inf'):
                self.events["firstStream"] = self.currentTime + self.generateFirstStreamInterarrivalTime()

            if self.events["secondStream"] == float('inf'):
                self.events["secondStream"] = self.currentTime + self.generateSecondStreamInterarrivalTime()

            # {"firstStream" : self.firstStreamQueue.pop() , "secondStream" : self.secondStreamQueue.pop() , "departure": self.currentTime + self.nextDepartureTime }
            minTimeEventKey = min(self.events, key=self.events.get)

            self.currentTime = self.events[minTimeEventKey]
            shouldRepeat = self.currentTime <= self.modelingTime

            if shouldRepeat:
                if minTimeEventKey == "firstStream" or minTimeEventKey == "secondStream":
                    self.arrivalEvent(minTimeEventKey, self.events[minTimeEventKey])
                    self.events[minTimeEventKey] = float('inf')
                else:
                    self.departureEvent(self.events[minTimeEventKey])

            self.updateStatistics(minTimeEventKey)

        pprint(self.serverUtilizationCoef())
        pprint(self.maxl1AmountInQueue)

        with open('statistics.csv', 'w') as csvfile:
            fieldnames = ['type', 'eventTime', 'L1', 'L2', 'departureTime', 'serverBusy', 'queueLength', 'queueContent']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

            writer.writeheader()
            writer.writerows(self.statistics)

    def departureEvent(self, time):
        self.itemServiceTime.append(self.serviceTime)
        if self.arrivalStack:
            arrivalType = self.arrivalStack.pop()
            if arrivalType == "firstStream": self.l1AmountInQueue -= 1
            self.serviceTime = self.generateServiceTime(arrivalType)
            self.events["departure"] = self.currentTime + self.serviceTime
        else:
            self.events["departure"] = float('inf')

    def arrivalEvent(self, typeOfStream, time):
        self.itemArrivalTime.append(time)
        if not self.arrivalStack and self.events["departure"] == float('inf'):
            self.serviceTime = self.generateServiceTime(typeOfStream)
            self.events["departure"] = self.currentTime + self.serviceTime
        else:
            if typeOfStream == "firstStream":
                self.l1AmountInQueue += 1
                if self.l1AmountInQueue > self.maxl1AmountInQueue: self.maxl1AmountInQueue = self.l1AmountInQueue
            self.arrivalStack.append(typeOfStream)

    def updateStatistics(self, typeOfEvent):
        event = {}
        event["type"] = typeOfEvent
        event["eventTime"] = self.currentTime
        event["L1"] = self.events["firstStream"]
        event["L2"] = self.events["secondStream"]
        event["departureTime"] = self.events["departure"]
        event["serverBusy"] = 0 if self.events["departure"] == float('inf') else 1
        event["queueLength"] = len(self.arrivalStack)
        event["queueContent"] = ' '.join(list(
            map(lambda arrivalType: "L1" if ("firstStream" == arrivalType) else "L2", self.arrivalStack)))
        self.statistics.append(event)

    def generateFirstStreamInterarrivalTime(self):
        return exponential(0.2)

    def generateSecondStreamInterarrivalTime(self):
        return erlang(2, 10)

    def serviceTimeFirstStream(self):
        return normal(20, 3)

    def serviceTimeSecondtStream(self):
        return exponential(0.2)

    def generateServiceTime(self, arrivalType):
         return self.serviceTimeFirstStream() if (arrivalType == "firstStream") else self.serviceTimeSecondtStream()

    def serverUtilizationCoef(self):
        serviceTimeSum = 0
        for time in self.itemServiceTime:
            serviceTimeSum += time
        return serviceTimeSum / self.modelingTime


simulation = Simulation(1000)
simulation.modeling()
