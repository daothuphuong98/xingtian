import sys
sys.path.append(r"simulator/dpdp_competition")
import gc
gc.collect()
from ortools.constraint_solver import routing_enums_pb2
from ortools.constraint_solver import pywrapcp
from ortools_opt.data_model import create_data_model
from src.utils.logging_engine import logger
import json
import traceback

TIMEOUT_PENALTY = 2_000
DROPPABLE_PENALTY = 6_000


def print_solution(data, manager, routing, solution):
    """Prints solution on console."""
    # objective
    print(f'Objective: {solution.ObjectiveValue()}')
    # Display dropped nodes.
    dropped_nodes = 'Dropped nodes:'
    for node in range(routing.Size()):
        if routing.IsStart(node) or routing.IsEnd(node):
            continue
        if solution.Value(routing.NextVar(node)) == node:
            dropped_nodes += f'{manager.IndexToNode(node)} '
    print(dropped_nodes)
    # Display routes
    capacity_dimension = routing.GetDimensionOrDie('Capacity')
    time_dimension = routing.GetDimensionOrDie('Time')
    total_lateness = 0
    total_distance = 0
    total_time = 0
    for vehicle_id in range(manager.GetNumberOfVehicles()):
        index = routing.Start(vehicle_id)
        plan_output = f'Route for vehicle {vehicle_id}:\n'
        route_distance = 0
        while not routing.IsEnd(index):
            time_var = time_dimension.CumulVar(index)
            node_index = manager.IndexToNode(index)
            route_load = data['demands'][node_index]
            lateness = data["time_windows"][node_index] - solution.Min(time_var)
            if lateness < 0:
                total_lateness += lateness
            plan_output += f' {node_index} Load({route_load}, {lateness}) -> '
            previous_index = index
            previous_node_index = manager.IndexToNode(previous_index)
            index = solution.Value(routing.NextVar(index))
            node_index = manager.IndexToNode(index)
            distance = data['distance_matrix'][previous_node_index][node_index]
            route_distance += distance
            total_distance += distance
            total_time += data['time_matrix'][previous_node_index][node_index]
        node_index = manager.IndexToNode(index)
        load_var = capacity_dimension.CumulVar(index)
        route_load = solution.Value(load_var)
        plan_output += f' {node_index} Load({route_load})\n'
        plan_output += f'Distance of the route: {route_distance}m\n'
        print(plan_output)
    print('Total Lateness:', total_lateness, '\nTotal Distance:', total_distance,'\nTotal Time:', total_time)

def solution_to_json(data, manager, routing, solution):
    location_to_factory = data['locations'].set_index('id').to_dict(orient='index')
    destination_output = {}
    planned_route_output = {}

    for vehicle in range(manager.GetNumberOfVehicles()):
        vehicle_id = f'V_{vehicle+1}'
        vehicle_info = data['vehicles'][vehicle]
        index = solution.Value(routing.NextVar(routing.Start(vehicle)))
        plan_output = []
        while not routing.IsEnd(index):
            node_index = manager.IndexToNode(index)
            factory = location_to_factory[node_index]

            output = {'factory_id': factory['factory_id'],
                      'lat': factory['latitude'],
                      'lng': factory['longitude'],
                      'delivery_item_list': [],
                      'pickup_item_list': [],
                      'arrive_time': 0,
                      'leave_time': 0}
            if isinstance(factory['order_id'], str):
                if factory['demand'] > 0:
                    output['pickup_item_list'] += data['items_to_orders'][factory['order_id']]
                else:
                    output['delivery_item_list'] += data['items_to_orders'][factory['order_id']]
            output['delivery_item_list'] = output['delivery_item_list'][::-1]

            # Add location to planned route if vehicle has never been to location and location is not destination
            if output and (node_index not in data['initial_routes'][vehicle] or location_to_factory[node_index].get('dest', 0) >= 1):
                plan_output.append(output)
            index = solution.Value(routing.NextVar(index))

        if len(plan_output) == 0:
            destination_output[vehicle_id] = None
            planned_route_output[vehicle_id] = []
        else:
            # Deduplicate locations
            previous_factory = plan_output[0]
            ind = 1
            while ind < len(plan_output):
                factory = plan_output[ind]
                if factory['factory_id'] == previous_factory['factory_id']:
                    dup_factory = plan_output.pop(ind)
                    plan_output[ind-1]['delivery_item_list'] += dup_factory['delivery_item_list']
                    plan_output[ind-1]['pickup_item_list'] += dup_factory['pickup_item_list']
                else:
                    ind += 1
                previous_factory = factory

            destination_output[vehicle_id] = plan_output[0]
            if vehicle_info['destination'] is not None:
                assert vehicle_info['destination']['factory_id'] == destination_output[vehicle_id]['factory_id']
                destination_output[vehicle_id]['arrive_time'] = vehicle_info['destination']['arrive_time']
            planned_route_output[vehicle_id] = plan_output[1:]

    with open('algorithm/data_interaction/output_destination.json', 'w') as f:
        json.dump(destination_output, f, indent=4)
    with open('algorithm/data_interaction/output_route.json', 'w') as f:
        json.dump(planned_route_output, f, indent=4)

def main():
    data = create_data_model()

    manager = pywrapcp.RoutingIndexManager(
        len(data["distance_matrix"]), data["num_vehicles"], data['starts'], data['ends']
    )

    routing = pywrapcp.RoutingModel(manager)

    # Add LIFO constraint
    routing.SetPickupAndDeliveryPolicyOfAllVehicles(
        pywrapcp.RoutingModel.PICKUP_AND_DELIVERY_LIFO
    )

    # Distance
    # Create and register a transit callback.
    def distance_callback(from_index, to_index):
        """Returns the distance between the two nodes."""
        # Convert from routing variable Index to distance matrix NodeIndex.
        from_node = manager.IndexToNode(from_index)
        to_node = manager.IndexToNode(to_index)
        return data['distance_matrix'][from_node][to_node]

    dist_transit_callback_index = routing.RegisterTransitCallback(distance_callback)

    # Define cost of each arc.
    routing.SetArcCostEvaluatorOfAllVehicles(dist_transit_callback_index)

    # Add Distance constraint.
    distance = 'Distance'
    routing.AddDimension(
        dist_transit_callback_index,
        0,  # no slack
        1_000_000,  # vehicle maximum travel distance
        True,  # start cumul to zero
        distance)
    distance_dimension = routing.GetDimensionOrDie(distance)
    # distance_dimension.SetGlobalSpanCostCoefficient(10)

    # Time
    # Create and register a transit callback.
    def time_callback(from_index, to_index):
        """Returns the distance between the two nodes."""
        # Convert from routing variable Index to distance matrix NodeIndex.
        from_node = manager.IndexToNode(from_index)
        to_node = manager.IndexToNode(to_index)
        return data['time_matrix'][from_node][to_node] + data['service_time'][to_node]

    time_transit_callback_index = routing.RegisterTransitCallback(time_callback)

    # Define cost of each arc.
    routing.SetArcCostEvaluatorOfAllVehicles(time_transit_callback_index)

    # Add Distance constraint.
    time = 'Time'
    routing.AddDimension(
        time_transit_callback_index,
        0,  # no slack
        10_000_000,  # vehicle maximum travel time
        True,  # start cumul to zero
        time)
    time_dimension = routing.GetDimensionOrDie(time)

    # Add soft time window constraint
    for location_idx, time_window, demand in zip(range(len(data['demands'])), data['time_windows'], data['demands']):
        if time_window > 0:
            index = manager.NodeToIndex(location_idx)
            if demand < 0:
                time_dimension.SetCumulVarSoftUpperBound(index, time_window, TIMEOUT_PENALTY)
            # if demand > 0:
            #     time_dimension.CumulVar(index).SetRange(0, time_window)

    # Capacity
    def demand_callback(from_index):
        """Returns the demand of the node."""
        # Convert from routing variable Index to demands NodeIndex.
        from_node = manager.IndexToNode(from_index)
        return data["demands"][from_node]

    demand_callback_index = routing.RegisterUnaryTransitCallback(demand_callback)
    routing.AddDimensionWithVehicleCapacity(
        demand_callback_index,
        0,  # null capacity slack
        data["vehicle_capacities"],  # vehicle maximum capacities
        True,  # start cumul to zero
        "Capacity",
    )

    # Pickup and Delivery
    for request in data["pickups_deliveries"]:
        pickup_index = manager.NodeToIndex(request[0])
        delivery_index = manager.NodeToIndex(request[1])
        routing.AddPickupAndDelivery(pickup_index, delivery_index)
        routing.solver().Add(
            routing.VehicleVar(pickup_index) == routing.VehicleVar(delivery_index)
        )
        routing.solver().Add(
            distance_dimension.CumulVar(pickup_index)
            <= distance_dimension.CumulVar(delivery_index)
        )

    # Add new orders as droppable nodes
    for node in data['droppable']:
        routing.AddDisjunction(
            [manager.NodeToIndex(node)],
            DROPPABLE_PENALTY) 

    for vehicle, initial_route in enumerate(data['initial_routes']):
        if len(initial_route) > 0:
            # Set next location of Start index
            start_index = routing.Start(vehicle)
            routing.NextVar(start_index).SetValues([start_index, manager.NodeToIndex(initial_route[0])])

            for loc, next_loc in zip(initial_route, initial_route[1:]):
                index_first = manager.NodeToIndex(loc)
                index_second = manager.NodeToIndex(next_loc)
                # Set next location
                routing.NextVar(index_first).SetValues([index_first, index_second])
                # Set vehicle
                routing.VehicleVar(index_first).SetValue(vehicle)

    search_parameters = pywrapcp.DefaultRoutingSearchParameters()
    search_parameters.first_solution_strategy = (
        routing_enums_pb2.FirstSolutionStrategy.PATH_CHEAPEST_ARC)
    search_parameters.local_search_metaheuristic = (
        routing_enums_pb2.LocalSearchMetaheuristic.GUIDED_LOCAL_SEARCH)
    
    search_parameters.time_limit.FromSeconds(10)
    search_parameters.use_full_propagation = False
    # search_parameters.log_search = True
    # search_parameters.use_depth_first_search = True
    search_parameters.guided_local_search_lambda_coefficient = 1
    # search_parameters.savings_max_memory_usage_bytes = 3e9

    routing.CloseModelWithParameters(search_parameters)

    # ReadAssignmentFromRoutes will close the model...
    # previous_solution = [[manager.NodeToIndex(node) for node in route]for route in data['previous_solution']]
    # previous_solution = routing.ReadAssignmentFromRoutes(previous_solution, True)
    # print('Initial solution:')
    # print_solution(data, manager, routing, previous_solution)

    # Solve the problem.
    # solution = routing.SolveFromAssignmentWithParameters(previous_solution, search_parameters)
    solution = routing.SolveWithParameters(search_parameters)
    print("Solver status: ", routing.status())

    # Print solution on console.
    if solution:
        print_solution(data, manager, routing, solution)
        gc.collect()
        print("SUCCESS")
        solution_to_json(data, manager, routing, solution)
    else:
        raise Exception('Cannot find solution!')

if __name__ == '__main__':
    try:
        main()
    except Exception as e:
        logger.error("Failed to run algorithm")
        logger.error(f"Error: {e}, {traceback.format_exc()}")
        print(e)
        print("FAIL")
