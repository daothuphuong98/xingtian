import numpy as np
from src.conf.configs import Configs
import pandas as pd
import json
from ortools_opt.config import OrtoolsConfigs


def get_location_id():
    num = 0
    while True:
        yield num
        num += 1


def create_data_model():
    """Stores the data for the problem."""
    data = {}
    config = Configs()
    ortools_config = OrtoolsConfigs()
    location_id_generator = get_location_id()

    routes = pd.read_csv(config.route_info_file_path)
    factories = pd.read_csv(config.factory_info_file_path)
    with open(config.algorithm_items_to_orders) as f:
        prev_split_order_lookup = json.load(f)
    with open(config.algorithm_vehicle_input_info_path) as f:
        vehicles = json.load(f)
    with open(config.algorithm_unallocated_order_items_input_path) as f:
        unallocated_orders = json.load(f)
    with open(config.algorithm_ongoing_order_items_input_path) as f:
        ongoing_orders = json.load(f)
        
    for order in ongoing_orders + unallocated_orders:
        order['order_id'] = prev_split_order_lookup.get(order['id'], order['order_id'])
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
    for vehicle in vehicles:
        initial_route = []
        # Get unique list of orders from item list
        orders = list(dict.fromkeys([prev_split_order_lookup.get(item_id, item_id.split('-')[0]) for item_id in vehicle['carrying_items']]))
        for order_id in orders:
            # Get real order id as splitted order has id <index>_<real_order_id> 
            demand = 0
            service_time = 0
            temp_item_id = None
            for item in vehicle['carrying_items']:
                if item.startswith(order_id) or prev_split_order_lookup.get(item) == order_id:
                    demand += id_to_ongoing_orders[item]['demand']
                    service_time += id_to_ongoing_orders[item]['load_time']
                    temp_item_id = item
            pickup_id = next(location_id_generator)
            pickup_location = {'id': pickup_id,
                               'order_id': order_id,
                               'factory_id': vehicle['cur_factory_id'],
                               'demand': demand}
            compulsory_locations.append(pickup_location)
            initial_route.append(pickup_id)

            deli_id = next(location_id_generator)
            deli_location = {'id': deli_id, 
                             'order_id': order_id,
                             'factory_id': id_to_ongoing_orders[temp_item_id]['delivery_factory_id'], 
                             'demand': -demand,
                             'service_time': service_time,
                             'time_constraint': max(id_to_ongoing_orders[temp_item_id]['committed_completion_time'] - update_time, 1)}
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

    # Add order_id to item_id dictionary
    og_order_id = {item: order['order_id'] for item, order in id_to_ongoing_orders.items()}
    data['items_to_orders'] = {}
    for item, order in og_order_id.items():
        if order in data['items_to_orders']:
            data['items_to_orders'][order].append(item)
        else:
            data['items_to_orders'][order] = [item]

    # Unallocated orders
    # Split order with capacity > 15
    unallocated_orders = pd.DataFrame(unallocated_orders)
    split_order_lookup = {}
    if len(unallocated_orders) > 0:
        unallocated_orders_demand = unallocated_orders.groupby('order_id')['demand'].sum().reset_index()
        for order_id, order in unallocated_orders_demand[unallocated_orders_demand['demand'] > ortools_config.MAX_ORDER_DEMAND]:
            splits = 0
            demand_checks = 0
            for item_id, item in unallocated_orders[unallocated_orders['order_id'] == order_id].iterrows():
                if demand_checks + item['demand'] > ortools_config.SPLIT_THRESHOLD:
                    demand_checks = 0
                    splits += 1
                unallocated_orders.loc[item_id, 'order_id'] = str(splits) + '_' + item['order_id']
                split_order_lookup[item['id']] = str(splits) + '_' + item['order_id']
                demand_checks += item['demand']

        # Get list of order after splitting (all orders have capacity < 15)
        ua_pickups_deliveries = unallocated_orders.groupby(
            ['order_id', "pickup_factory_id", "delivery_factory_id", 'committed_completion_time']).agg({'demand': 'sum', 'load_time': 'sum'}).reset_index()

        #Add dictionary items to orders
        data['items_to_orders'].update(unallocated_orders.groupby('order_id')['id'].apply(list).to_dict())

        # Add pickup and delivery factories of each unallocated orders to location list as droppable locations:
        for ind, ua_order in ua_pickups_deliveries.iterrows():
            time_to_order = routes[(routes['start_factory_id'] == ua_order['pickup_factory_id']) 
                                    & (routes['end_factory_id'] == ua_order['delivery_factory_id'])]
            pickup_id = next(location_id_generator)
            pickup_location = {'id': pickup_id,
                            'order_id': ua_order['order_id'],
                            'factory_id': ua_order['pickup_factory_id'],
                            'demand': ua_order['demand'],
                            'service_time': ua_order['load_time'],
                            'time_to_order': time_to_order.iloc[0,-1],
                            'time_constraint': ua_order['committed_completion_time'] - update_time}
            droppable_locations.append(pickup_location)

            deli_id = next(location_id_generator)
            deli_location = {'id': deli_id, 
                             'order_id': ua_order['order_id'],
                            'factory_id': ua_order['delivery_factory_id'], 
                            'demand': -ua_order['demand'],
                            'service_time': ua_order['load_time'],
                            'time_constraint': max(ua_order['committed_completion_time'] - update_time, 1)}
            droppable_locations.append(deli_location)

            data["pickups_deliveries"].append([pickup_id, deli_id])
    
    split_order_lookup.update(prev_split_order_lookup)

    # Convert start factories, compulsory factories and droppable factories as pandas DF
    start_locations = pd.DataFrame(start_locations)
    compulsory_locations = pd.DataFrame(compulsory_locations)
    compulsory_locations['droppable'] = 0
    droppable_locations = pd.DataFrame(droppable_locations)
    if len(droppable_locations) > 0:
        droppable_locations['droppable'] = droppable_locations.apply(lambda x: 0 if (x['time_to_order'] + Configs.DOCK_APPROACHING_TIME) * ortools_config.COMPULSORY_ORDER_THRESHOLD > x['time_constraint'] and x['demand'] > 0 else 1, axis=1)

    # Join all 3 locations into 1
    locations = pd.concat([start_locations, compulsory_locations, droppable_locations]).drop_duplicates()
    locations['dummy'] = 1
    locations = locations.merge(factories, on='factory_id', how='left')
    data['demands'] = (locations['demand'] * ortools_config.DEMAND_COEF).astype(int).tolist()

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
                delivery_orders = []
                for item_id in next_location['delivery_item_list']:
                    order_id = item_id.split('-')[0]
                    if not delivery_orders_to_location.get(order_id):
                        order_id = split_order_lookup.get(item_id)
                    if order_id and not order_id in delivery_orders:
                        delivery_orders.append(order_id)
                delivery_location = [delivery_orders_to_location[ids] for ids in delivery_orders if ids in delivery_orders_to_location]
                prev_solutions_location += delivery_location

                pickup_orders = []
                for item_id in next_location['pickup_item_list']:
                    order_id = item_id.split('-')[0]
                    if not pickup_orders_to_location.get(order_id):
                        order_id = split_order_lookup.get(item_id)
                    if order_id and not order_id in pickup_orders:
                        pickup_orders.append(order_id)
                pickup_location = [pickup_orders_to_location[ids] for ids in pickup_orders]
                prev_solutions_location += pickup_location

        previous_solution.append(prev_solutions_location)

    # Set distance and time of start location and already picked-up location as 0
    zero_location = [item for sublist in data['initial_routes'] for item in sublist]
    for loc_idx in zero_location:
        if locations['dest'][loc_idx] == 1:
            continue
        distance_matrix.iloc[:, loc_idx] = 0
        distance_matrix.iloc[loc_idx, :] = 0

        time_matrix.iloc[:, loc_idx] = 0
        time_matrix.iloc[loc_idx, :] = 0
    
    # Load data in to data dict.
    data['distance_matrix'] = (distance_matrix*ortools_config.DISTANCE_COEF).astype(int).values
    time_matrix[time_matrix > 0] += Configs.DOCK_APPROACHING_TIME
    data['time_matrix'] = time_matrix.astype(int).values

    data['locations'] = locations
    data['previous_solution'] = [list(dict.fromkeys(initial_route + solution)) for solution, initial_route in zip(previous_solution, data['initial_routes'])]

    data["num_vehicles"] = len(vehicles)
    data['vehicles'] = vehicles
    data["vehicle_capacities"] = [int(vehicle['capacity']*ortools_config.DEMAND_COEF) for vehicle in vehicles]
    data["starts"] = start_locations['id'].tolist()[1:]
    data['ends'] = [0 for _ in vehicles]
    data['droppable'] = locations.loc[locations['droppable'] == 1, 'id'].astype(int).tolist()
    data['time_windows'] = locations['time_constraint'].fillna(0).astype(int).tolist()
    data['service_time'] = locations['service_time'].fillna(0).astype(int).tolist()

    # Return a dictionary of item_id to order_id for look-up in case of order splitting
    with open(config.algorithm_items_to_orders, 'w') as f:
        json.dump(split_order_lookup, f, indent=4)

    return data


if __name__ == '__main__':
    create_data_model()
