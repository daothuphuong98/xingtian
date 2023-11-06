# Copyright (C) 2021. Huawei Technologies Co., Ltd. All rights reserved.
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
# THE SOFTWARE

import copy
# from dpdp_competition.algorithm.algorithm_demo_split_duplicate import check_queueing

from numpy.core.numeric import Inf

from src.common.node import Node
from src.common.route import Map
from src.conf.configs import Configs
from src.utils.input_utils import get_factory_info, get_route_map
from src.utils.json_tools import convert_nodes_to_json
from src.utils.json_tools import get_vehicle_instance_dict, get_order_item_dict
from src.utils.json_tools import read_json_from_file, write_json_to_file
from src.utils.logging_engine import logger

# import pickle as pkl
# from scipy.optimize import linear_sum_assignment
import numpy as np


# with open('/home/ava/workspace2/xingtian/simulator/dpdp_competition/nodes_dist_time_index.pkl', 'rb') as f:
#     nodes_dist_index = pkl.load(f)

def __calculate_demand(item_list: list):
    demand = 0
    for item in item_list:
        demand += item.demand
    return demand


def __get_capacity_of_vehicle(id_to_vehicle: dict):
    for vehicle_id, vehicle in id_to_vehicle.items():
        return vehicle.board_capacity


def __create_pickup_and_delivery_nodes_of_items(items: list, id_to_factory: dict):
    pickup_factory_id = __get_pickup_factory_id(items)
    delivery_factory_id = __get_delivery_factory_id(items)
    if len(pickup_factory_id) == 0 or len(delivery_factory_id) == 0:
        return None, None

    pickup_factory = id_to_factory.get(pickup_factory_id)
    delivery_factory = id_to_factory.get(delivery_factory_id)
    pickup_node = Node(pickup_factory.id, pickup_factory.lng, pickup_factory.lat, copy.copy(items), [])

    delivery_items = []
    last_index = len(items) - 1
    for i in range(len(items)):
        delivery_items.append(items[last_index - i])
    delivery_node = Node(delivery_factory.id, delivery_factory.lng, delivery_factory.lat, [], copy.copy(delivery_items))
    return pickup_node, delivery_node


def __get_pickup_factory_id(items):
    if len(items) == 0:
        logger.error("Length of items is 0")
        return ""

    factory_id = items[0].pickup_factory_id
    for item in items:
        if item.pickup_factory_id != factory_id:
            logger.error("The pickup factory of these items is not the same")
            return ""

    return factory_id


def __get_delivery_factory_id(items):
    if len(items) == 0:
        logger.error("Length of items is 0")
        return ""

    factory_id = items[0].delivery_factory_id
    for item in items:
        if item.delivery_factory_id != factory_id:
            logger.error("The delivery factory of these items is not the same")
            return ""

    return factory_id


# 合并相邻重复节点 Combine adjacent-duplicated nodes.
# def __combine_duplicated_nodes(nodes):
#     n = 0
#     while n < len(nodes)-1:
#         if nodes[n].id == nodes[n+1].id:
#             nodes[n].pickup_items.extend(nodes.pop(n+1).pickup_items)
#         n += 1

def __combine_duplicated_nodes(nodes):
    n = 0
    while n < len(nodes) - 1:
        if nodes[n].id == nodes[n + 1].id:
            # nodes[n].pickup_items.extend(nodes.pop(n+1).pickup_items)
            nodes[n].pickup_items.extend(nodes[n + 1].pickup_items)
            nodes[n].delivery_items.extend(nodes.pop(n + 1).delivery_items)
            n -= 1
        n += 1


def update_incoming_car(id_to_vehicle, cur_time):
    incoming_car = {}
    for vehicle_id, vehicle in id_to_vehicle.items():
        if vehicle.cur_factory_id == '' and vehicle.destination is not None and not vehicle.carrying_items.is_empty():
            if vehicle.destination.arrive_time - cur_time <= 600:
                try:
                    incoming_car[vehicle.destination.id].append(vehicle_id)
                except:
                    incoming_car[vehicle.destination.id] = [vehicle_id]
    return incoming_car


def assign_incoming_car(route_str, tmp_items, incoming_car, id_to_factory, vehicle_id_to_destination,
                        vehicle_id_to_planned_route):
    try:
        vid = incoming_car[route_str.split(',')[0]].pop(0)
        tmp_items.reverse()
        pickup_node, delivery_node = __create_pickup_and_delivery_nodes_of_items(tmp_items, id_to_factory)
        # destination = pickup_node
        vehicle_id_to_destination[vid] = pickup_node
        # vehicle_id_to_planned_route['V_' + str(v_index)].append(pickup_node)

        vehicle_id_to_planned_route[vid].append(pickup_node)
        vehicle_id_to_planned_route[vid].append(delivery_node)
        print('-~-~-~- Assigned an incoming car: ' + str(vid) + '-~-~-~-')
        print(' ****** Current tmp_items ********')
        for vv in tmp_items:
            print(vv.order_id)
        return 1, vehicle_id_to_destination, vehicle_id_to_planned_route
    except:
        return 0, vehicle_id_to_destination, vehicle_id_to_planned_route


def assign_unfull_car(route_map, unfull_car, nodeid, route_str, tmp_items, cur_cap, id_to_factory,
                      vehicle_id_to_destination, vehicle_id_to_planned_route):
    # if route_str.split(',')[0] == nodeid:
    if route_str.split(',')[0] in list(unfull_car):
        nodeid = route_str.split(',')[0]
        min_dist = 1e5
        vmin = 0
        for vidx, v in unfull_car[nodeid].items():
            if v + cur_cap <= 15:
                print('debuging')
                print(unfull_car[nodeid])
                print(vidx)
                print(vehicle_id_to_planned_route[vidx])
                print(nodeid)
                print(list(unfull_car))
                try:
                    old_delivery_node = vehicle_id_to_planned_route[vidx][-1]
                    pickup_node, delivery_node = __create_pickup_and_delivery_nodes_of_items(tmp_items, id_to_factory)
                    cur_dist = route_map.calculate_transport_time_between_factories(old_delivery_node.id,
                                                                                    delivery_node.id)
                    if cur_dist < min_dist:
                        vid = vidx
                        min_dist = cur_dist
                        vmin = v
                    continue
                except:
                    continue
            else:
                continue
        try:
            print('Unfull car ' + str(vid) + ': ' + str(vmin))
            print(cur_cap)
            tmp_items.reverse()
            pickup_node, delivery_node = __create_pickup_and_delivery_nodes_of_items(tmp_items, id_to_factory)
            vehicle_id_to_planned_route[vid].insert(len(vehicle_id_to_planned_route[vid]) // 2, pickup_node)
            vehicle_id_to_planned_route[vid].insert((len(vehicle_id_to_planned_route[vid]) + 1) // 2, delivery_node)
            print('-.-.-.- Assigned an unfull car: ' + str(vid) + '-.-.-.-')
            print(' ****** Current tmp_items ********')
            for vv in tmp_items:
                print(vv.order_id)
            if unfull_car[nodeid][vid] + cur_cap < 15:
                unfull_car[nodeid][vid] += cur_cap
            else:
                del unfull_car[nodeid][vid]
                if len(unfull_car[nodeid]) == 0:
                    del unfull_car[nodeid]
            return 1, unfull_car, vehicle_id_to_destination, vehicle_id_to_planned_route
        except:
            return 0, unfull_car, vehicle_id_to_destination, vehicle_id_to_planned_route
    else:
        return 0, unfull_car, vehicle_id_to_destination, vehicle_id_to_planned_route


def record_unfull_vehicles(topsite, cur_time, id_to_vehicle):
    unfull_car = {}
    dock_used = {}
    cnt = 0
    for vehicle_id, vehicle in id_to_vehicle.items():
        if vehicle.destination is not None and len(vehicle.destination.pickup_items) > 0:
            if len(vehicle.destination.pickup_items) < 15:  # unfull
                v_cap = 0
                for items in vehicle.destination.pickup_items:
                    v_cap += items.demand
                if vehicle.destination.id == topsite:
                    thres = 15
                else:
                    thres = 9
                if v_cap > thres:
                    try:
                        unfull_car[vehicle.destination.id][vehicle_id] = v_cap
                    except:
                        unfull_car[vehicle.destination.id] = {}
                        unfull_car[vehicle.destination.id][vehicle_id] = v_cap
        if vehicle.cur_factory_id == topsite and vehicle.leave_time_at_current_factory - cur_time > 0:
            dock_used[cnt] = vehicle_id
            cnt += 1
    return unfull_car, dock_used


def update_unfull_car(route_str, unfull_car, vid, route_cap):
    if route_str.split(',')[0] in list(unfull_car):
        nodeid = route_str.split(',')[0]
        if vid in list(unfull_car[nodeid]):
            unfull_car[nodeid][vid] = route_cap
    return unfull_car


def update_unfull_car_2(unfull_car, vid, route_cap, route_str, nodeid):
    if route_str.split(',')[0] in list(unfull_car):
        nodeid = route_str.split(',')[0]
        if vid not in list(unfull_car[nodeid]):
            if route_cap < 15:
                unfull_car[nodeid][vid] = route_cap
    return unfull_car


def record_driving_cars(driving_cars, vid, vehicle, cur_time, id_to_vehicle):
    # driving_cars[vid] =
    v_cap = 0
    for items in vehicle.get_unloading_sequence():
        v_cap += items.demand
    if v_cap < 15:
        driving_cars[vid] = [v_cap, (cur_time - vehicle.get_unloading_sequence[0].creation_time)]
    return driving_cars


def topsite_waittime(topsite, cur_time, id_to_vehicle):
    max_wait = 0
    for vehicle_id, vehicle in id_to_vehicle.items():
        if vehicle.cur_factory_id == topsite and vehicle.leave_time_at_current_factory - cur_time > 0:
            cur_wait = vehicle.leave_time_at_current_factory - vehicle.arrive_time_at_current_factory - 1800 - 3600
            if cur_wait > max_wait:
                max_wait = cur_wait
    return max_wait


# def check_assigned_items(route_str, node_id_1, node_id_2, tmp_items, route_capacity, vehicle_capacity, id_to_factory, vehicle_id_to_planned_route):
#    try:
#        if route_str.split(',')[0] == node_id_1 and route_str.split(',')[1] != node_id_2 and vehicle_capacity[vid] < 15:
#            pickup_node, delivery_node = __create_pickup_and_delivery_nodes_of_items(tmp_items, id_to_factory)
#            vehicle_id_to_planned_route[vid].insert(len(vehicle_id_to_planned_route[vid])//2, pickup_node)
#            vehicle_id_to_planned_route[vid].insert((len(vehicle_id_to_planned_route[vid]) + 1)//2, delivery_node)
#        vehicle_capacity[vid] += route_capacity
#    except:
#        vehicle_capacity[vid] = route_capacity


def new_dispatch(id_to_unallocated_order_item, id_to_vehicle, id_to_factory, route_map):
    vehicle_id_to_destination = {}
    vehicle_id_to_planned_route = dict()
    for i in range(1, len(list(id_to_vehicle)) + 1):
        vehicle_id_to_planned_route['V_' + str(i)] = []

    # dealing with the carrying items of vehicles (处理车辆身上已经装载的货物)
    for vehicle_id, vehicle in id_to_vehicle.items():
        unloading_sequence_of_items = vehicle.get_unloading_sequence()
        # print("## Unloading sequence: {}".format(unloading_sequence_of_items))

        vehicle_id_to_planned_route[vehicle_id] = []
        if len(unloading_sequence_of_items) > 0:
            delivery_item_list = []
            factory_id = unloading_sequence_of_items[0].delivery_factory_id
            for item in unloading_sequence_of_items:
                if item.delivery_factory_id == factory_id:
                    delivery_item_list.append(item)
                else:
                    factory = id_to_factory.get(factory_id)
                    node = Node(factory_id, factory.lng, factory.lat, [], copy.copy(delivery_item_list))
                    vehicle_id_to_planned_route[vehicle_id].append(node)
                    delivery_item_list = [item]
                    factory_id = item.delivery_factory_id
            if len(delivery_item_list) > 0:
                factory = id_to_factory.get(factory_id)
                node = Node(factory_id, factory.lng, factory.lat, [], copy.copy(delivery_item_list))
                vehicle_id_to_planned_route[vehicle_id].append(node)

    # for the empty vehicle, it has been allocated to the order, but have not yet arrived at the pickup factory
    pre_matching_item_ids = []
    preallocate_route = {}
    for vehicle_id, vehicle in id_to_vehicle.items():
        if vehicle.carrying_items.is_empty() and vehicle.destination is not None:
            pickup_items = vehicle.destination.pickup_items
            # pickup_node, delivery_node = __create_pickup_and_delivery_nodes_of_items(pickup_items, id_to_factory)
            # vehicle_id_to_planned_route[vehicle_id].append(pickup_node)
            # vehicle_id_to_planned_route[vehicle_id].append(delivery_node)
            pre_matching_item_ids.extend([item.id for item in pickup_items])
            print('Preallocated vehicle: ' + str(vehicle_id))
            try:
                if vehicle_id not in preallocate_route[
                    pickup_items[0].pickup_factory_id + ',' + pickup_items[0].delivery_factory_id]:
                    preallocate_route[
                        pickup_items[0].pickup_factory_id + ',' + pickup_items[0].delivery_factory_id].append(
                        vehicle_id)
            except:
                preallocate_route[pickup_items[0].pickup_factory_id + ',' + pickup_items[0].delivery_factory_id] = [
                    vehicle_id]

    # update_incoming_car
    cur_time = id_to_vehicle['V_1'].gps_update_time
    incoming_car = update_incoming_car(id_to_vehicle, cur_time)

    # update_unfull_car
    topsite = '2445d4bd004c457d95957d6ecf77f759'
    # unfull_car, dock_used = record_unfull_vehicles(topsite, cur_time, id_to_vehicle)
    # max_wait = topsite_waittime(topsite, cur_time, id_to_vehicle)

    # Assign unallocated items
    destination = None
    # planned_route = []

    for vehicle_id, vehicle in id_to_vehicle.items():
        vehicle_id_to_destination[vehicle_id] = destination
    #     vehicle_id_to_planned_route[vehicle_id] = planned_route

    '''
        Order (items) can be Split or Not?
    '''
    can_split = {}
    cannot_split = {}
    try:
        old_order_id = id_to_unallocated_order_item[list(id_to_unallocated_order_item)[0]].order_id
    except:
        old_order_id = None
    now_order_demand = 0

    end_of_dict = len(list(id_to_unallocated_order_item)) - 1
    temp_cnt = 0
    for k, v in id_to_unallocated_order_item.items():
        if v.order_id == old_order_id and temp_cnt != end_of_dict:
            now_order_demand += v.demand
        elif v.order_id != old_order_id and temp_cnt != end_of_dict:
            if now_order_demand > 15:
                can_split[old_order_id] = now_order_demand
            else:
                cannot_split[old_order_id] = now_order_demand
            old_order_id = v.order_id
            now_order_demand = v.demand
        elif v.order_id == old_order_id and temp_cnt == end_of_dict:
            now_order_demand += v.demand
            if now_order_demand > 15:
                can_split[old_order_id] = now_order_demand
            else:
                cannot_split[old_order_id] = now_order_demand
        elif v.order_id != old_order_id and temp_cnt == end_of_dict:
            if now_order_demand > 15:
                can_split[old_order_id] = now_order_demand
            else:
                cannot_split[old_order_id] = now_order_demand
            old_order_id = v.order_id
            now_order_demand = v.demand
            if now_order_demand > 15:
                can_split[old_order_id] = now_order_demand
            else:
                cannot_split[old_order_id] = now_order_demand
        temp_cnt += 1

    '''
        For test
    '''
    top_distance = route_map.calculate_transport_time_between_factories('2445d4bd004c457d95957d6ecf77f759',
                                                                        '9f1a09c368584eba9e7f10a53d55caae')
    '''
        Start: Print Current Matching Info
    '''
    print('----- Current round information -----')
    # print(id_to_vehicle['V_1'])
    # print(id_to_vehicle['V_1'].gps_update_time)
    route_capacity = {}
    route_totaltime = {}
    route_min_create_time = {}
    curr_route_time = {}
    route_items = {}

    # For bug[not split]: to check whether an order will be splitted into two trips
    route_order_id = {}
    route_depth = {}

    # To preallocate vehicles and add items
    # preallocate_route = {}

    # Count in transport_time, load_time and unload_time
    # Count in cumulative demand
    # print(len(list(id_to_unallocated_order_item)))
    # print(id_to_unallocated_order_item[list(id_to_unallocated_order_item)[-1]].get_orderitem())
    try:
        old_order_id = id_to_unallocated_order_item[list(id_to_unallocated_order_item)[0]].order_id
    except:
        old_order_id = None
    for k, v in id_to_unallocated_order_item.items():
        # if k in pre_matching_item_ids:
        #     continue
        duplicate_str = ',' + v.pickup_factory_id + ',' + v.delivery_factory_id
        route_str = v.pickup_factory_id + ',' + v.delivery_factory_id
        try:
            route_depth[v.pickup_factory_id + ',' + v.delivery_factory_id]
        except:
            route_depth[v.pickup_factory_id + ',' + v.delivery_factory_id] = 0

        route_str += route_depth[v.pickup_factory_id + ',' + v.delivery_factory_id] * duplicate_str
        if v.order_id in list(cannot_split):
            # print('----- Current Item: ' + str(k) + ' -----')
            if v.order_id != old_order_id:
                cur_route_str = route_str
                # print('----- Current Order: ' + v.order_id + '; Previous: ' + old_order_id)
                # print('Cur_route_str: ' + str(cur_route_str))
                try:
                    while route_capacity[cur_route_str] + cannot_split[v.order_id] > 15:
                        # print('Cur_route_str: ' + str(cur_route_str))
                        duplicate_str = ',' + v.pickup_factory_id + ',' + v.delivery_factory_id
                        route_str = route_str + duplicate_str
                        cur_route_str = route_str

                    route_str = cur_route_str
                    route_capacity[route_str] += v.demand

                    route_totaltime[route_str] += v.load_time
                    route_totaltime[route_str] += v.unload_time

                    route_items[route_str].append(v)
                except:
                    route_capacity[route_str] = v.demand

                    # route_totaltime[route_str] = v.get_orderitem()['transport_time']
                    route_totaltime[route_str] = route_map.calculate_transport_time_between_factories(
                        v.pickup_factory_id, v.delivery_factory_id)
                    route_totaltime[route_str] += v.load_time
                    route_totaltime[route_str] += v.unload_time

                    route_min_create_time[route_str] = v.creation_time

                    route_items[route_str] = [v]
            else:
                try:
                    if k == list(id_to_unallocated_order_item)[0]:
                        cur_route_str = route_str
                    route_str = cur_route_str
                    route_capacity[route_str] += v.demand

                    route_totaltime[route_str] += v.load_time
                    route_totaltime[route_str] += v.unload_time

                    route_items[route_str].append(v)
                except:
                    route_capacity[route_str] = v.demand

                    # route_totaltime[route_str] = v.get_orderitem()['transport_time']
                    route_totaltime[route_str] = route_map.calculate_transport_time_between_factories(
                        v.pickup_factory_id, v.delivery_factory_id)
                    route_totaltime[route_str] += v.load_time
                    route_totaltime[route_str] += v.unload_time

                    route_min_create_time[route_str] = v.creation_time

                    route_items[route_str] = [v]

        elif v.order_id in list(can_split):
            try:
                while route_capacity[route_str] + v.demand > 15:
                    duplicate_str = ',' + v.pickup_factory_id + ',' + v.delivery_factory_id
                    route_str = route_str + duplicate_str

                route_capacity[route_str] += v.demand
                if route_capacity[route_str] == 15:
                    route_depth[v.pickup_factory_id + ',' + v.delivery_factory_id] += 1

                route_totaltime[route_str] += v.load_time
                route_totaltime[route_str] += v.unload_time

                route_items[route_str].append(v)

            except:
                route_capacity[route_str] = v.demand

                # route_totaltime[route_str] = v.get_orderitem()['transport_time']
                route_totaltime[route_str] = route_map.calculate_transport_time_between_factories(v.pickup_factory_id,
                                                                                                  v.delivery_factory_id)
                route_totaltime[route_str] += v.load_time
                route_totaltime[route_str] += v.unload_time

                route_min_create_time[route_str] = v.creation_time

                route_items[route_str] = [v]

        old_order_id = v.order_id
        # if k in list(preallocate_vehicle):
        #     try:
        #         # preallocate_route[route_str] = preallocate_vehicle[k]
        #         if preallocate_vehicle[k] not in preallocate_route[v.pickup_factory_id + ',' + v.delivery_factory_id]:
        #             preallocate_route[v.pickup_factory_id + ',' + v.delivery_factory_id].append(preallocate_vehicle[k])
        #     except:
        #         preallocate_route[v.pickup_factory_id + ',' + v.delivery_factory_id] = [preallocate_vehicle[k]]
        #     print('Assign preallocated vehicle ' + str(route_str))
    # print('======= Capacity ========')
    # print(list(route_capacity))
    # print(route_capacity.values())

    # ## To check if there is vehicle that is approaching the pickup_node
    # for vehicle_id, vehicle in id_to_vehicle.items():
    #     cur_vehicle_cap = 0
    #     if vehicle.carrying_items.is_empty() and vehicle.destination.id == vehicle.cur_factory_id:
    #         print('-*-*-*-*- Arrived and approaching -*-*-*-*-')
    #         pickup_items = vehicle.destination.pickup_items
    #         for item in pickup_items:
    #             cur_vehicle_cap += item.demand

    # Count in pickup distance
    vehicle_to_source = []
    avail_vehicle = list(np.arange(1, len(list(id_to_vehicle)) + 1))

    # for k,v in route_totaltime.items():
    #     for vid, vehicle in id_to_vehicle.items():
    #         unloading_sequence_of_items = vehicle.get_unloading_sequence()
    #         if len(unloading_sequence_of_items) > 0:
    #             if k.split(',')[0] == unloading_sequence_of_items[0].delivery_factory_id:
    #                 try:
    #                     preallocate_route[k.split(',')[0]+','+k.split(',')[1]]
    #                     continue
    #                 except:
    #                     print('Same location: ' + str(vid))
    #                     print('Order source_destination: ' + str(k))
    #                     print('Time to service: ' + str(vehicle.destination.leave_time - id_to_vehicle['V_1'].gps_update_time))

    for vid, vehicle in id_to_vehicle.items():
        unloading_sequence_of_items = vehicle.get_unloading_sequence()
        if len(unloading_sequence_of_items) > 0:
            # print(vehicle.cur_factory_id)
            # print(vehicle.destination.delivery_items[0].pickup_factory_id)
            '''
                20210708
            '''
            # if vehicle.cur_factory_id == '' or vehicle.cur_factory_id != '':
            #     print('location')
            #     print(vid + '$$')
            #     print('Time to leave this node: ' + str(vehicle.leave_time_at_current_factory - id_to_vehicle['V_1'].gps_update_time))
            #     print('Time to arrive: ' + str(vehicle.destination.arrive_time - id_to_vehicle['V_1'].gps_update_time))
            #     print('Time to service: ' + str(vehicle.destination.leave_time - id_to_vehicle['V_1'].gps_update_time))
            #     print(vehicle.destination.id)
            #     print(vehicle.cur_factory_id)
            #     print('pickup_id: ' + str(vehicle.destination.delivery_items[0].pickup_factory_id))
            #     print(len(vehicle.destination.delivery_items))
            print('Unloading_items')
            print(unloading_sequence_of_items[0].delivery_factory_id)
            print(vehicle.cur_factory_id)
            if vehicle.cur_factory_id == unloading_sequence_of_items[0].delivery_factory_id:
                print('location')
                print(vid + '$$')
                print('Time arrive this node: ' + str(
                    id_to_vehicle['V_1'].gps_update_time - vehicle.arrive_time_at_current_factory))
                print('Time to service: ' + str(
                    vehicle.leave_time_at_current_factory - id_to_vehicle['V_1'].gps_update_time))
                print('pickup_id: ' + str(vehicle.destination.delivery_items[0].pickup_factory_id))
                print(len(vehicle.destination.delivery_items))

    if len(list(route_totaltime)) > 0:
        # for i in range(1,6):
        for i in range(1, len(list(id_to_vehicle)) + 1):
            # if len(vehicle_id_to_planned_route.get('V_'+str(i))) > 0:
            # if id_to_vehicle['V_' + str(i)].destination is not None
            if id_to_vehicle['V_' + str(i)].destination is not None or id_to_vehicle[
                'V_' + str(i)].leave_time_at_current_factory != id_to_vehicle['V_' + str(i)].gps_update_time:
                print('current vehicle: V_' + str(i))
                avail_vehicle.remove(i)

        for i in range(len(avail_vehicle)):
            vehicle_to_source.append([])
            map_count = {}
            for j in range(len(list(route_totaltime))):
                # Preallocated vehicle
                try:
                    preallocate_route[
                        list(route_totaltime)[j].split(',')[0] + ',' + list(route_totaltime)[j].split(',')[1]]
                    try:
                        map_count[
                            list(route_totaltime)[j].split(',')[0] + ',' + list(route_totaltime)[j].split(',')[1]] += 1
                    except:
                        map_count[
                            list(route_totaltime)[j].split(',')[0] + ',' + list(route_totaltime)[j].split(',')[1]] = 1
                    if map_count[
                        list(route_totaltime)[j].split(',')[0] + ',' + list(route_totaltime)[j].split(',')[1]] > len(
                            preallocate_route[
                                list(route_totaltime)[j].split(',')[0] + ',' + list(route_totaltime)[j].split(',')[1]]):
                        pass
                    else:
                        # print('Hit ' +  str(i))
                        # print(list(route_totaltime)[j])
                        continue
                except:
                    pass

                source = id_to_vehicle['V_' + str(avail_vehicle[i])].cur_factory_id
                end = list(route_totaltime)[j].split(',')[0]
                if source == end:
                    pickup_time = 0
                else:
                    # pickup_time = nodes_dist_index[source + ',' + end][1]
                    pickup_time = route_map.calculate_transport_time_between_factories(source, end)
                vehicle_to_source[i].append(pickup_time)

        ## Apply KM algorithm
        vehicle_to_source = np.array(vehicle_to_source)
        # row, col = linear_sum_assignment(vehicle_to_source)
        # print('-- col --')
        # print(col)
    else:
        pass

    if len(vehicle_to_source) > 0 or len(list(preallocate_route)) > 0:
        # Count in max item_live_time
        cnt = 0
        # sort_by_time = sorted(route_totaltime.items(), key=lambda d: d[1], reverse=True)
        for k, v in route_totaltime.items():
            try:
                # v_index = preallocate_route[k]
                v_index = preallocate_route[k.split(',')[0] + ',' + k.split(',')[1]][0]
                print('Preallocated route')
                print('preallocate_vehicle_location: ' + str(
                    id_to_vehicle[v_index].cur_factory_id) + 'item_pickup_node: ' + str(
                    k.split(',')[0] + ',' + k.split(',')[1]))
                if id_to_vehicle[v_index].cur_factory_id != '':
                    print(id_to_vehicle[v_index].destination.id)
                    print(id_to_vehicle[v_index].carrying_items.is_empty())
                    print(vehicle_id_to_planned_route[str(v_index)])
                print('capacity <= 15: cap:' + str(route_capacity[k]))
                tmp_items = route_items[k]
                tmp_items.reverse()
                print(' ****** Assigned ' + str(v_index) + '********')
                print(' ****** Current tmp_items ********')
                for vv in tmp_items:
                    print(vv.order_id)

                preallocate_route[k.split(',')[0] + ',' + k.split(',')[1]].pop(0)
                if len(preallocate_route[k.split(',')[0] + ',' + k.split(',')[1]]) == 0:
                    del preallocate_route[k.split(',')[0] + ',' + k.split(',')[1]]
                print('pop')

                pickup_node, delivery_node = __create_pickup_and_delivery_nodes_of_items(tmp_items, id_to_factory)
                # destination = pickup_node
                vehicle_id_to_destination[str(v_index)] = pickup_node
                # vehicle_id_to_planned_route['V_' + str(v_index)].append(pickup_node)

                vehicle_id_to_planned_route[str(v_index)].append(pickup_node)
                vehicle_id_to_planned_route[str(v_index)].append(delivery_node)
                print('planned')

                # unfull_car = update_unfull_car(k, unfull_car,v_index,route_capacity[k])
                continue
            except:
                pass

            if len(vehicle_to_source) == 0:
                continue

                # print('========= Create time:' + str(route_min_create_time[k]) + '==========')
            temp_index = np.argmin(vehicle_to_source[:, cnt])
            print(
                'Min pickup time: ' + str(np.min(vehicle_to_source[:, cnt])) + ';  V_' + str(avail_vehicle[temp_index]))

            # print('=====LEN=====')
            # print(len(vehicle_to_source))
            # print(len(vehicle_to_source[0]))
            # print(temp_index)

            # temp_index = np.argmin(vehicle_to_source[:,cnt])
            v_index = avail_vehicle[temp_index]
            source = id_to_vehicle['V_' + str(v_index)].cur_factory_id
            end = k.split(',')[0]
            if source == end:
                pickup_time = 0
            else:
                # pickup_time = nodes_dist_index[source + ',' + end][1]
                pickup_time = route_map.calculate_transport_time_between_factories(source, end)
            curr_route_time[k] = v + pickup_time
            cnt += 1

            # Add max live time
            curr_route_time[k] += id_to_vehicle['V_1'].gps_update_time - route_min_create_time[k]

            # Add condition checking:
            # Distance avoidance: len(avail_vehicle) >= len(list(route_totaltime))
            # Out of time avoidance: len(avail_vehicle) < len(list(route_totaltime))
            # if len(list(id_to_unallocated_order_item)) < 50:
            #     thres = 3000
            # elif len(list(id_to_unallocated_order_item)) < 100:
            #     thres = 5000
            if len(avail_vehicle) >= len(list(route_totaltime)):
                thres = 11000
            elif len(avail_vehicle) < len(list(route_totaltime)):
                print('+++ Out of time avoidance +++')
                print(len(avail_vehicle))
                print(len(list(route_totaltime)))
                thres = 11000

            # cap_thres = 9
            cap_thres = 9 - pickup_time // 600
            topsite = '2445d4bd004c457d95957d6ecf77f759'
            max_live_time_thre = Inf
            if k.split(',')[0] == topsite:
                # thres = 11000 - route_capacity[k]*240
                cap_thres = 15 - pickup_time // 600
                # if len(list(dock_used)) > 15:
                # if max_wait > 10000:
                #     thres = 15000
                # print('=== This Round: # wait_time = ' + str(max_wait) + '  ===')
                # max_live_time_thre = 1200

            # hotsite
            # hotsites = ['2445d4bd004c457d95957d6ecf77f759', '9b40bfd6ca1c432498685540652a5a8b','f6faef4b36e743328800b961aced4a2c', 'b6dd694ae05541dba369a2a759d2c2b9', '5e2e9efa5ade4984bb18af66028bc0c8','ffd0ed8719f54294a452ed3e3b6a986c']
            # hotsites = ['2445d4bd004c457d95957d6ecf77f759', '9b40bfd6ca1c432498685540652a5a8b','f6faef4b36e743328800b961aced4a2c', 'b6dd694ae05541dba369a2a759d2c2b9']
            # last_update = cur_time - route_items[k][-1].creation_time
            # if k.split(',')[1] in hotsites and last_update > 3600:
            #     cap_thres = 1

            # Check condition: value + 600 > 13400:
            # Add max_live_time check:
            max_live_time = id_to_vehicle['V_1'].gps_update_time - route_min_create_time[k]

            #
            # unfull_flag, unfull_car, vehicle_id_to_destination, vehicle_id_to_planned_route = assign_unfull_car(route_map, unfull_car, topsite, k, route_items[k], route_capacity[k], id_to_factory,vehicle_id_to_destination, vehicle_id_to_planned_route)
            unfull_flag = 0

            #
            if unfull_flag == 0:
                income_flag, vehicle_id_to_destination, vehicle_id_to_planned_route = assign_incoming_car(k,
                                                                                                          route_items[
                                                                                                              k],
                                                                                                          incoming_car,
                                                                                                          id_to_factory,
                                                                                                          vehicle_id_to_destination,
                                                                                                          vehicle_id_to_planned_route)
            else:
                income_flag = 0

            # if curr_route_time[k] + 600 >= thres and route_capacity[k] < cap_thres:
            #     with open('/home/ava/workspace2/xingtian/simulator/dpdp_competition/cold_route.txt', 'a') as f:
            #         f.write(str(k)+ ',' + str(route_capacity[k]) + ',' + str(route_items[k][-1].creation_time - cur_time) + '\n')
            # elif route_capacity[k] >= cap_thres:
            #     with open('/home/ava/workspace2/xingtian/simulator/dpdp_competition/hot_route.txt', 'a') as f:
            #         f.write(str(k.split(',')[0] + ',' + k.split(',')[1])+'\n')

            if (curr_route_time[k] + 600 >= thres or route_capacity[
                k] >= cap_thres or max_live_time > max_live_time_thre) and income_flag == 0 and unfull_flag == 0:
                # unfull_flag, unfull_car, vehicle_id_to_destination, vehicle_id_to_planned_route = assign_unfull_car(route_map, unfull_car, topsite, k, route_items[k], route_capacity[k], id_to_factory,vehicle_id_to_destination, vehicle_id_to_planned_route)
                # destination = k.split(',')[0]
                if route_capacity[k] > 15:
                    print('Large Order!!')
                    print(k)
                    cur_demand = 0
                    # pre_demand = 0
                    total_demand = 0
                    pre_total_demand = 0

                    cur_order_id = route_items[k][0].order_id
                    pre_order_id = route_items[k][0].order_id

                    cur_order_demand = 0
                    i_begin = 0
                    i_end = 0
                    cap_end = -1
                    tmp_items = []

                    for i in range(len(route_items[k])):
                        # print('pre: ' + str(pre_demand))

                        cur_order_id = route_items[k][i].order_id
                        total_demand += route_items[k][i].demand
                        if total_demand > 15 and pre_total_demand <= 15:
                            cap_end = i
                            print('cap_end:' + str(cap_end))
                        pre_total_demand = total_demand

                        if cur_order_id == pre_order_id and i != len(route_items[k]) - 1:
                            print('pppp')
                            print(i)
                            print(cur_order_id)
                            cur_order_demand += route_items[k][i].demand
                            pre_order_id = cur_order_id
                            i_end = i
                        elif cur_order_id == pre_order_id and i == len(route_items[k]) - 1:
                            print('pppp')
                            print(i)
                            print(cur_order_id)
                            cur_order_demand += route_items[k][i].demand
                            if cur_order_demand <= 15:
                                print('CAN NOT split')
                                print(cur_order_id)
                                print(pre_order_id)
                                print('i_end:' + str(i_end))
                                print('cap_end:' + str(cap_end))
                                break
                            elif cur_order_demand > 15:
                                print('CAN split')
                                print(cur_order_id)
                                print('i_end:' + str(i_end))
                                ## can be split
                                tmp_items += route_items[k][i_begin:cap_end]
                                break
                        elif cap_end >= i_end + 1:
                            print('cap_end > i_end')
                            print('i_begin:' + str(i_begin))
                            print('i_end:' + str(i_end))
                            print('cur_order_demand: ' + str(cur_order_demand))
                            print('cur_order_id:' + str(cur_order_id))
                            print('pre_order_id:' + str(pre_order_id))
                            tmp_items += route_items[k][i_begin:i_end + 1]
                            break
                        elif cap_end != -1 and cap_end < i_end + 1:
                            if cur_order_demand <= 15:
                                print('CAN NOT split')
                                print(cur_order_id)
                                print(pre_order_id)
                                print('i_end:' + str(i_end))
                                print('cap_end:' + str(cap_end))
                                print('total_demand:' + str(total_demand))
                                print('cur_order_demand:' + str(cur_order_demand))
                                ## can not be split: skip
                                total_demand -= cur_order_demand
                                if total_demand > 15:
                                    break
                                else:
                                    pre_total_demand = total_demand
                                    # cap_end = len(route_items[k]) + 1
                                    cap_end = -1
                                    i_begin = i
                                    i_end = i
                                    cur_order_demand = route_items[k][i].demand
                                    pre_order_id = cur_order_id
                            elif cur_order_demand > 15:
                                print('CAN split')
                                print(cur_order_id)
                                print('i_end:' + str(i_end))
                                ## can be split
                                tmp_items += route_items[k][i_begin:cap_end]
                                print(tmp_items)
                                break
                        elif cap_end == -1:
                            print('ADD')
                            print(i)
                            print(cur_order_id)
                            print(total_demand)
                            tmp_items += route_items[k][i_begin:i_end + 1]
                            i_begin = i
                            i_end = i
                            cur_order_demand = route_items[k][i].demand
                            pre_order_id = cur_order_id
                            if total_demand >= 15:
                                print('ADD: Exit')
                                break

                # else:
                elif unfull_flag == 0:
                    print('capacity <= 15: cap:' + str(route_capacity[k]))
                    tmp_items = route_items[k]
                    tmp_items.reverse()

                print(' ****** Assigned V_' + str(v_index) + '********')
                print(' ****** Current tmp_items ********')
                for vv in tmp_items:
                    print(vv.order_id)

                pickup_node, delivery_node = __create_pickup_and_delivery_nodes_of_items(tmp_items, id_to_factory)
                # destination = pickup_node
                vehicle_id_to_destination['V_' + str(v_index)] = pickup_node
                # vehicle_id_to_planned_route['V_' + str(v_index)].append(pickup_node)

                vehicle_id_to_planned_route['V_' + str(v_index)].append(pickup_node)
                vehicle_id_to_planned_route['V_' + str(v_index)].append(delivery_node)

                # update_unfull_car
                # if thres - curr_route_time[k] - 600 >= 3000 and route_capacity[k] >= cap_thres:
                #     unfull_car = update_unfull_car_2(unfull_car, 'V_' + str(v_index), route_capacity[k], k, topsite)

                # remove v_index
                avail_vehicle.pop(temp_index)
                vehicle_to_source = np.delete(vehicle_to_source, temp_index, axis=0)
                # vehicle_to_source.pop(np.argmin(vehicle_to_source[:,cnt]))

                if len(vehicle_to_source) == 0 and len(list(preallocate_route)) == 0:
                    break

        print(route_capacity.values())
        print(route_totaltime.values())
        print(curr_route_time.values())

    else:
        print('No available vehicle')

    # create the output of the algorithm
    for vehicle_id, vehicle in id_to_vehicle.items():
        origin_planned_route = vehicle_id_to_planned_route.get(vehicle_id)
        # print('---Origin===')
        # print(origin_planned_route)
        # Combine adjacent-duplicated nodes.
        __combine_duplicated_nodes(origin_planned_route)

        destination = None
        planned_route = []
        # determine the destination
        if vehicle.destination is not None:
            if len(origin_planned_route) == 0:
                logger.error(f"Planned route of vehicle {vehicle_id} is wrong")
            else:
                destination = origin_planned_route[0]
                destination.arrive_time = vehicle.destination.arrive_time
                planned_route = [origin_planned_route[i] for i in range(1, len(origin_planned_route))]
        elif len(origin_planned_route) > 0:
            destination = origin_planned_route[0]
            planned_route = [origin_planned_route[i] for i in range(1, len(origin_planned_route))]

        # print('@ destination: {}'.format(destination))
        # print('@ planned_route: {}'.format(planned_route))

        vehicle_id_to_destination[vehicle_id] = destination
        vehicle_id_to_planned_route[vehicle_id] = planned_route

    # print(vehicle_id_to_destination)
    # print(vehicle_id_to_planned_route)

    return vehicle_id_to_destination, vehicle_id_to_planned_route

    '''
        End: Print Current Matching Info
    '''


"""

Main body
# Note
# This is the demo to show the main flowchart of the algorithm

"""


def scheduling():
    # read the input json, you can design your own classes
    id_to_factory, id_to_unallocated_order_item, id_to_ongoing_order_item, id_to_vehicle, route_map = __read_input_json()

    # vehicle_id_to_destination, vehicle_id_to_planned_route = dispatch_orders_to_vehicles(
    #     id_to_unallocated_order_item,
    #     id_to_vehicle,
    #     id_to_factory)

    vehicle_id_to_destination, vehicle_id_to_planned_route = new_dispatch(
        id_to_unallocated_order_item,
        id_to_vehicle,
        id_to_factory,
        route_map)

    # print('--- round end --')
    # print(vehicle_id_to_destination)
    # print(vehicle_id_to_planned_route)

    # dispatching algorithm

    '''
        Start: Print Current Matching Info
    '''
    # print('----- Current round information -----')
    # print('@Input: id_to_factory: {}'.format(len(id_to_factory)))
    # print('@Input: id_to_vehicle: {}'.format(len(id_to_vehicle)))
    # print(id_to_vehicle.keys())
    # for i in range(1,6):
    #     print('---- V_' + str(i) + ': ' + str(len(id_to_vehicle['V_'+str(i)].carrying_items.items))
    #     + '; Capacity: ' + str(id_to_vehicle['V_'+str(i)].board_capacity)
    #     + '; Serving time: ' + str(id_to_vehicle['V_'+str(i)].operation_time)
    #     # + '; Gps_id: ' + str(id_to_vehicle['V_'+str(i)].gps_id)
    #     )
    #     print(id_to_vehicle['V_'+str(i)].carrying_items.get_item_details())
    # print('@Input: id_to_unallocated_order_item: {}'.format(len(id_to_unallocated_order_item)))
    # print(id_to_unallocated_order_item.keys())
    # print('@Output: vehicle_id_to_destination: {}'.format(vehicle_id_to_destination))
    # print('@Output: vehicle_id_to_planned_route: {}'.format(vehicle_id_to_planned_route))
    # temp = []
    # temp_duration = []
    # for i in id_to_unallocated_order_item.values():
    #     temp.append(i.get_orderitem()['transport_time'])
    #     temp_duration.append(i.get_orderitem()['duration'])
    # print(temp)
    # print(temp_duration)
    # print(route_map)
    # print(vehicle_id_to_destination)
    # print(vehicle_id_to_planned_route)
    '''
        End: Print Current Matching Info
    '''

    # output the dispatch result
    __output_json(vehicle_id_to_destination, vehicle_id_to_planned_route)


def __read_input_json():
    # read the factory info
    id_to_factory = get_factory_info(Configs.factory_info_file_path)

    # read the route map
    code_to_route = get_route_map(Configs.route_info_file_path)
    route_map = Map(code_to_route)

    # read the input json, you can design your own classes
    unallocated_order_items = read_json_from_file(Configs.algorithm_unallocated_order_items_input_path)
    id_to_unallocated_order_item = get_order_item_dict(unallocated_order_items, 'OrderItem')

    ongoing_order_items = read_json_from_file(Configs.algorithm_ongoing_order_items_input_path)
    id_to_ongoing_order_item = get_order_item_dict(ongoing_order_items, 'OrderItem')

    id_to_order_item = {**id_to_unallocated_order_item, **id_to_ongoing_order_item}

    vehicle_infos = read_json_from_file(Configs.algorithm_vehicle_input_info_path)
    id_to_vehicle = get_vehicle_instance_dict(vehicle_infos, id_to_order_item, id_to_factory)

    return id_to_factory, id_to_unallocated_order_item, id_to_ongoing_order_item, id_to_vehicle, route_map


def __output_json(vehicle_id_to_destination, vehicle_id_to_planned_route):
    write_json_to_file(Configs.algorithm_output_destination_path, convert_nodes_to_json(vehicle_id_to_destination))
    write_json_to_file(Configs.algorithm_output_planned_route_path, convert_nodes_to_json(vehicle_id_to_planned_route))
