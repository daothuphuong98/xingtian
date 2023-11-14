import os

class OrtoolsConfigs(object):
    # If time remained to complete order < COMPULSORY_ORDER_THRESHOLD * time required to complete order -> order is compulsory and must not be dropped
    COMPULSORY_ORDER_THRESHOLD = 2

    # If order demand > MAX_ORDER_DEMAND -> order must be splitted
    MAX_ORDER_DEMAND = 15 
    # If order must be splitted, order will be splitted into smaller orders with demand = SPLIT_THRESHOLD
    SPLIT_THRESHOLD = 5

    #Demand coefficient (demand = DEMAND_COEF * real demand to make sure demand is an integer)
    DEMAND_COEF = 100
    DISTANCE_COEF = 10

    #Penalty
    TIMEOUT_PENALTY = 100
    DROPPABLE_PENALTY = 100_000

    #Time limit
    TIME_LIMIT = 120