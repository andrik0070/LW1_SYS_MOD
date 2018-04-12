import numpy as np
from random import uniform
from math import sqrt, log, pow


def exponential(lam):
    return (-1 / lam) * np.log(uniform(0, 1.0))


def erlang(k, lam):
    number = 1.0
    for x in range(1, k):
        number = (1.0 - uniform(0, 1.0))
    return -lam * np.log(number)


def normal(mean, std):
    x = uniform(-1, 1)
    y = uniform(-1, 1)
    s = pow(x, 2) + pow(y, 2)
    while s > 1 or s == 0:
        x = uniform(-1, 1)
        y = uniform(-1, 1)
        s = (pow(x, 2)) + (pow(y, 2))
    z = x * sqrt((-2 * log(s)) / s)
    return mean + std * z
