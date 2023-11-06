import numpy as np
from src.conf.configs import Configs
import pandas as pd
import json


def get_location_id():
    num = 0
    while True:
        yield num
        num += 1


def create_data_model():
    """Stores the data for the problem."""
    data = {}
    config = Configs()
    location_id_generator = get_location_id()

    routes = pd.read_csv(config.route_info_file_path)
    factories = pd.read_csv(config.factory_info_file_path)
    with open(config.algorithm_vehicle_input_info_path) as f:
        vehicles = json.load(f)
    with open(config.algorithm_unallocated_order_items_input_path) as f:
        unallocated_orders = json.load(f)
    with open(config.algorithm_ongoing_order_items_input_path) as f:
        ongoing_orders = json.load(f)
        id_to_ongoing_orders = {order['id']: order for order in ongoing_orders}

    update_time = vehicles[0]['update_time']

    # Start location contains end depot in advance
    depot = {'id': next(location_id_generator),
             'factory_id': '0',
             'demand': 0}
    start_locations = [depot]
    compulsory_locations = []
    droppable_locations = []
    data['initial_routes'] = []
    data["pickups_deliveries"] = []

    # On-going orders
    # Add start locations of each factory to location list
    # If vehicle is in transit (no cur_factory_id), start location is depot
    for vehicle in vehicles:
        if len(vehicle['cur_factory_id']) == 0:
            vehicle['cur_factory_id'] = vehicle['destination']['factory_id']
        start_location = {'id': next(location_id_generator),
                          'factory_id': vehicle['cur_factory_id'],
                          'demand': 0}
        start_locations.append(start_location)

    # Add already picked-up orders to location list as compulsory locations
    for start_loc, vehicle in zip(start_locations[1:], vehicles):
        initial_route = []
        # Get unique list of orders from item list
        orders = list(dict.fromkeys([item_id.split('-')[0] for item_id in vehicle['carrying_items']]))
        for order_id in orders:
            demand = 0
            for item in vehicle['carrying_items']:
                if item.startswith(order_id):
                    demand += id_to_ongoing_orders[item]['demand']
            pickup_id = next(location_id_generator)
            pickup_location = {'id': pickup_id,
                               'order_id': order_id,
                               'factory_id': vehicle['cur_factory_id'],
                               'demand': demand}
            compulsory_locations.append(pickup_location)
            initial_route.append(pickup_id)

            deli_id = next(location_id_generator)
            deli_location = {'id': deli_id, 'order_id': order_id,
                             'factory_id': id_to_ongoing_orders[item]['delivery_factory_id'], 'demand': -demand,
                             'time_constraint': id_to_ongoing_orders[item]['committed_completion_time'] - update_time}
            compulsory_locations.append(deli_location)
            data["pickups_deliveries"].append([pickup_id, deli_id])

        if vehicle['destination'] is not None:
            dest_id = next(location_id_generator)
            dest_location = {'id': dest_id,
                             'order_id': None,
                             'factory_id': vehicle['destination']['factory_id'],
                             'demand': 0,
                             'time_constraint': None,
                             'dest': 1}
            compulsory_locations.append(dest_location)
            initial_route.append(dest_id)
        data['initial_routes'].append(initial_route)

    # Unallocated orders
    # Split order with capacity > 15
    unallocated_orders = pd.DataFrame(unallocated_orders)
    # unallocated_orders['order_split_id'] = unallocated_orders['order_id']
    unallocated_orders_demand = unallocated_orders.groupby('order_id')['demand'].sum().reset_index()
    for order in unallocated_orders_demand[unallocated_orders_demand['demand'] > vehicles[0]['capacity']].groupby(
            'order_id'):
        splits = 0
        demand_checks = 0
        for item in order.iteritems():
            if demand_checks + item['demand'] > vehicles[0]['capacity']:
                demand_checks = 0
                splits += 1
            item['order_id'] = str(splits) + '_' + item['order_id']
            demand_checks += item['demand']

    # Get list of order after splitting (all orders have capacity < 15)
    ua_pickups_deliveries = unallocated_orders.groupby(
        ['order_id', "pickup_factory_id", "delivery_factory_id", 'committed_completion_time'])[
        'demand'].sum().reset_index()

    #Add dictionary items to orders
    data['items_to_orders'] = unallocated_orders.groupby('order_id')['id'].apply(list).to_dict()
    og_order_id = {item: order['order_id'] for item, order in id_to_ongoing_orders.items()}
    og_order_id2 = {}
    for item, order in og_order_id.items():
        if order in og_order_id2:
            og_order_id2[order].append(item)
        else:
            og_order_id2[order] = [item]
    data['items_to_orders'].update(og_order_id2)

    # Add pickup and delivery factories of each unallocated orders to location list as droppable locations:
    for ind, ua_order in ua_pickups_deliveries.iterrows():
        pickup_id = next(location_id_generator)
        pickup_location = {'id': pickup_id,
                           'order_id': ua_order['order_id'],
                           'factory_id': ua_order['pickup_factory_id'],
                           'demand': ua_order['demand']}
        droppable_locations.append(pickup_location)

        deli_id = next(location_id_generator)
        deli_location = {'id': deli_id, 'order_id': ua_order['order_id'],
                         'factory_id': ua_order['delivery_factory_id'], 'demand': -ua_order['demand'],
                         'time_constraint': ua_order['committed_completion_time'] - update_time}
        droppable_locations.append(deli_location)

        data["pickups_deliveries"].append([pickup_id, deli_id])

    # Convert start factories, compulsory factories and droppable factories as pandas DF
    start_locations = pd.DataFrame(start_locations)
    compulsory_locations = pd.DataFrame(compulsory_locations)
    compulsory_locations['droppable'] = 0
    droppable_locations = pd.DataFrame(droppable_locations)
    droppable_locations['droppable'] = 1

    # Join all 3 locations into 1
    locations = pd.concat([start_locations, compulsory_locations, droppable_locations]).drop_duplicates()
    locations['dummy'] = 1
    locations = locations.merge(factories, on='factory_id', how='left')
    data['demands'] = (locations['demand'] * 100).astype(int).tolist()

    # Create distance and time matrix
    route_df = locations[['factory_id', 'dummy', 'id']].merge(locations[['factory_id', 'dummy', 'id']],
                                                              on='dummy').drop(columns='dummy')
    route_df.columns = ['start_factory_id', 'start_id', 'end_factory_id', 'end_id']
    route_df = route_df.merge(routes, on=['start_factory_id', 'end_factory_id'], how='left')
    distance_matrix = route_df.pivot(index='start_id', columns='end_id',
                                     values='distance').fillna(0)
    time_matrix = route_df.pivot(index='start_id', columns='end_id',
                                 values='time').fillna(0)

    # Load previous solutions
    with open(config.algorithm_output_destination_path) as f:
        destination = json.load(f)
    with open(config.algorithm_output_planned_route_path) as f:
        planned_route = json.load(f)
    previous_solution = []
    pickup_orders_to_location = dict(
        zip(locations[locations['demand'] > 0].order_id, locations[locations['demand'] > 0].id))
    delivery_orders_to_location = dict(
        zip(locations[locations['demand'] < 0].order_id, locations[locations['demand'] < 0].id))
    for vehicle in vehicles:
        vehicle_id = vehicle['id']
        prev_solutions = [destination[vehicle_id]] + planned_route[vehicle_id]
        prev_solutions_location = []

        for next_location in prev_solutions:
            if next_location:
                delivery_orders = list(
                    dict.fromkeys([item_id.split('-')[0] for item_id in next_location['delivery_item_list']]))
                delivery_location = [delivery_orders_to_location[ids] for ids in delivery_orders if ids in delivery_orders_to_location]
                prev_solutions_location += delivery_location

                pickup_orders = list(dict.fromkeys([item_id.split('-')[0] for item_id in next_location['pickup_item_list']]))
                pickup_location = [pickup_orders_to_location[ids] for ids in pickup_orders]
                prev_solutions_location += pickup_location

        previous_solution.append(prev_solutions_location)

    data['distance_matrix'] = (distance_matrix*10).astype(int).values
    data['time_matrix'] = time_matrix.astype(int).values
    data['locations'] = locations
    previous_solution = [list(dict.fromkeys(initial_route + solution)) for solution, initial_route in zip(previous_solution, data['initial_routes'])]
    data['previous_solution'] = previous_solution

    data["num_vehicles"] = len(vehicles)
    data['vehicles'] = vehicles
    data["vehicle_capacities"] = [int(vehicle['capacity']*100) for vehicle in vehicles]
    data["starts"] = start_locations['id'].tolist()[1:]
    data['ends'] = [0 for _ in vehicles]
    data['droppable'] = locations.loc[locations['droppable'] == 1, 'id'].tolist()
    data['time_windows'] = locations['time_constraint'].fillna(0).astype(int).tolist()

    return data


if __name__ == '__main__':
    create_data_model()
