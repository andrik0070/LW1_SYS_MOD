import sys
import numpy as np
import queue
from arrival import Arrival
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
        self.itemDelayTimeSumForFirstStream = 0
        self.itemDelayTimeSumForSecondStream = 0
        self.serviceTimeSumFirstStream = 0
        self.serviceTimeSumSecondStream = 0
        self.totalAmountOfFirstStreamItems = 0
        self.totalAmountOfSecondStreamItems = 0
        self.serviceTime = None
        self.events = {"firstStream": self.currentTime + self.generateFirstStreamInterarrivalTime(),
                       "secondStream": self.currentTime + self.generateSecondStreamInterarrivalTime(),
                       "departure": float('inf')}
        self.statistics = []
        self.l1AmountInQueue = 0
        self.maxl1AmountInQueue = 0
        self.currentlyServicedArrival = None
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
                    arrival = Arrival(minTimeEventKey, self.events[minTimeEventKey])
                    self.arrivalEvent(arrival)
                    self.events[minTimeEventKey] = float('inf')
                else:
                    self.departureEvent()
                self.updateStatistics(minTimeEventKey)


        if self.serviceTime != float('inf'):
            if self.currentlyServicedArrival.typeOfStream == 'firstStream':
               self.serviceTimeSumFirstStream += self.modelingTime - self.currentlyServicedArrival.serviceStartTime
            else:
               self.serviceTimeSumSecondStream += self.modelingTime - self.currentlyServicedArrival.serviceStartTime


        while self.arrivalStack:
            arrival = self.arrivalStack.pop()
            if arrival.typeOfStream == 'firstStream':
                self.itemDelayTimeSumForFirstStream += self.modelingTime - arrival.time
            else:
                self.itemDelayTimeSumForSecondStream += self.modelingTime - arrival.time






        pprint(self.serverUtilizationCoef())
        pprint(self.maxl1AmountInQueue)

        firstStreamItemsAverageDelay = self.itemDelayTimeSumForFirstStream / self.totalAmountOfFirstStreamItems
        secondStreamItemsAverageDelay = self.itemDelayTimeSumForSecondStream / self.totalAmountOfSecondStreamItems

        pprint(firstStreamItemsAverageDelay)
        pprint(secondStreamItemsAverageDelay)


        totalAmountOfItems = self.totalAmountOfFirstStreamItems + self.totalAmountOfSecondStreamItems
        averageDelay = (self.itemDelayTimeSumForFirstStream + self.itemDelayTimeSumForSecondStream) / totalAmountOfItems
        pprint(averageDelay)

        pprint((self.totalAmountOfFirstStreamItems/500) * firstStreamItemsAverageDelay)
        pprint((self.totalAmountOfSecondStreamItems / 500) * secondStreamItemsAverageDelay)
        pprint((totalAmountOfItems/500)*averageDelay)

        averageServiceTimeFirstStream = (self.serviceTimeSumFirstStream + self.itemDelayTimeSumForFirstStream ) / self.totalAmountOfFirstStreamItems
        averageServiceTimeSecondStream = (self.serviceTimeSumSecondStream + self.itemDelayTimeSumForSecondStream ) / self.totalAmountOfSecondStreamItems

        pprint(averageServiceTimeFirstStream)
        pprint(averageServiceTimeSecondStream)

        pprint((self.serviceTimeSumFirstStream + self.serviceTimeSumSecondStream + self.itemDelayTimeSumForFirstStream + self.itemDelayTimeSumForSecondStream ) / (self.totalAmountOfFirstStreamItems + self.totalAmountOfSecondStreamItems))


        with open('statistics.csv' , 'w') as csvfile:
            fieldnames = ['type', 'eventTime', 'L1', 'L2', 'departureTime', 'serverBusy', 'queueLength', 'queueContent']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames,dialect='excel')

            writer.writeheader()
            writer.writerows(self.statistics)

    def departureEvent(self):
        self.itemServiceTime.append(self.serviceTime)

        if self.currentlyServicedArrival:
          if  self.currentlyServicedArrival.typeOfStream == 'firstStream':
              self.serviceTimeSumFirstStream += self.serviceTime
          else:
              self.serviceTimeSumSecondStream += self.serviceTime

        if self.arrivalStack:
            arrival = self.arrivalStack.pop()
            arrival.serviceStartTime = self.currentTime
            self.currentlyServicedArrival = arrival
            if arrival.typeOfStream == "firstStream":
                self.itemDelayTimeSumForFirstStream = self.itemDelayTimeSumForFirstStream + self.currentTime - arrival.time
                self.l1AmountInQueue -= 1
            else:
                self.itemDelayTimeSumForSecondStream = self.itemDelayTimeSumForSecondStream + self.currentTime - arrival.time
            self.serviceTime = self.generateServiceTime(arrival.typeOfStream)
            self.events["departure"] = self.currentTime + self.serviceTime
        else:
            self.events["departure"] = float('inf')

    def arrivalEvent(self, arrival):
        self.itemArrivalTime.append(arrival.time)
        if arrival.typeOfStream == "firstStream":
            self.totalAmountOfFirstStreamItems = self.totalAmountOfFirstStreamItems + 1
        else:
            self.totalAmountOfSecondStreamItems = self.totalAmountOfSecondStreamItems + 1

        if not self.arrivalStack and self.events["departure"] == float('inf'):
            self.serviceTime = self.generateServiceTime(arrival.typeOfStream)
            self.currentlyServicedArrival = arrival
            self.currentlyServicedArrival.serviceStartTime = arrival.time
            self.events["departure"] = self.currentTime + self.serviceTime
        else:
            if arrival.typeOfStream == "firstStream":
                self.l1AmountInQueue += 1
                if self.l1AmountInQueue > self.maxl1AmountInQueue: self.maxl1AmountInQueue = self.l1AmountInQueue
            self.arrivalStack.append(arrival)

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

    def serviceTimeSecondStream(self):
        return exponential(0.2)

    def generateServiceTime(self, arrivalType):
         return self.serviceTimeFirstStream() if (arrivalType == "firstStream") else self.serviceTimeSecondStream()

    def serverUtilizationCoef(self):
        serviceTimeSum = 0
        for time in self.itemServiceTime:
            serviceTimeSum += time
        return serviceTimeSum / self.modelingTime


simulation = Simulation(500)
simulation.modeling()
