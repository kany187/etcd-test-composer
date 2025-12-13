#!/usr/bin/env -S python3 -u

# This file serves as a parallel driver (https://antithesis.com/docs/test_templates/test_composer_reference/#parallel-driver). 
# It does between 1 and 100 random kv puts against a random etcd host in the cluster. We then check to see if successful puts persisted
# and are correct on another random etcd host.

# Antithesis SDK
from antithesis.assertions import (
    always,
    sometimes,
)

import sys
sys.path.append("/opt/antithesis/resources")
import helper


def simulate_traffic():
    """
        This function will first connect to an etcd host, then execute a certain number of put requests. 
        The key and value for each put request are generated using Antithesis randomness (check within the helper.py file). 
        We return the key/value pairs from successful requests.
    """
    client = helper.connect_to_host()
    num_requests = helper.generate_requests()
    kvs = []

    for _ in range(num_requests):

        # generating random str for the key and value
        key = helper.generate_random_string()
        value = helper.generate_random_string()

        # response of the put request
        success, error = helper.put_request(client, key, value)

        # Antithesis Assertion: sometimes put requests are successful. A failed request is OK since we expect them to happen.
        sometimes(success, "Client can make successful put requests", {"error":error})

        if success:
            kvs.append((key, value))
            print(f"Client: successful put with key '{key}' and value '{value}'")
        else:
            print(f"Client: unsuccessful put with key '{key}', value '{value}', and error '{error}'")

    print(f"Client: traffic simulated!")
    return kvs
    

def validate_puts(kvs):
    """
        This function will first connect to an etcd host, then perform a get request on each key in the key/value array. 
        For each successful response, we check that the get request value == value from the key/value array. 
        If we ever find a mismatch, we return it. 
    """
    client = helper.connect_to_host()

    for kv in kvs:
        key, value = kv[0], kv[1]
        success, error, database_value = helper.get_request(client, key)

        # Antithesis Assertion: sometimes get requests are successful. A failed request is OK since we expect them to happen.
        sometimes(success, "Client can make successful get requests", {"error":error})

        if not success:
            print(f"Client: unsuccessful get with key '{key}', and error '{error}'")
        elif value != database_value:
            print(f"Client: a key value mismatch! This shouldn't happen.")
            return False, (value, database_value)

    print(f"Client: validation ok!")
    return True, None

def validate_monotonic_reads(kvs):
    """
        Test monotonic reads: version numbers should never decrease accross reads.

        For each key that was successfully written, we read it multiple times
        from different etcd nodes and verify that version numbers only go forward
    """

    print('Client: starting monotonuc reads violations')

    if not kvs:
        print('Client: no keys to test, skipping monotonic reads check')
        return True, {"total_reads": 0}
    
    highest_version_seen = {}
    total_reads = 0
    problems_found = 0

    #Test the first 5 keys
    keys_to_test = kvs[:5]

    print(f"Client: testing {len(keys_to_test)} keys for monotonic reads")

    for key_value_pair in keys_to_test:
        key = key_value_pair[0]
        highest_version_seen[key] = -1

        print(f"Client: testing key: '{key}'")

        #Read this key 5 times from different clusters
        for attempt in range(5):
            client = helper.connect_to_host()

            current_version = helper.get_key_version(client, key)

            if current_version is not None:
                last_version = highest_version_seen[key]
                total_reads += 1

                print(f"Client: attempt {attempt+1}: version={current_version} (last was {last_version})")

                went_backwards = current_version < last_version

                if went_backwards:
                    problems_found += 1
                    print(f"Client: monotonic read violation - version went from {last_version} to {current_version}")
                
                #Antithesis Assertion: version numbers must never decrease
                always(
                    not went_backwards,
                    "Version numbers must never decrease (monotonic reads)",
                    {
                        "key": key,
                        "current_version": current_version,
                        "previous_version": last_version,
                        "went_backwards": went_backwards
                    }
                )

                highest_version_seen[key] = max(highest_version_seen[key], current_version)

    # Antithesis Assertion: sometimes monotonic read checks are successful
    sometimes(
        total_reads > 0,
        "Client can make successful monotonic read checks",
        {"total_reads": total_reads}
    )

    success = (problems_found == 0)
    if success:
        print(f"Client: monotonic reads validation passed - ({total_reads} successful reads)")
    else:
        print(f"Client: monotonic reads validation failed - ({problems_found} violations found)")
    
    return success, {
        "total_reads": total_reads,
        "problems_found": problems_found,
        "keys_tested": len(keys_to_test)
    }



if __name__ == "__main__":
    kvs = simulate_traffic()
    values_stay_consistent, mismatch = validate_puts(kvs)

    # Antithesis Assertion: for all successful kv put requests, values from get requests should match for their respective keys 
    always(values_stay_consistent, "Database key values stay consistent", {"mismatch":mismatch})

    monotonic_ok, monotonic_details = validate_monotonic_reads(kvs)
    
    # Antithesis Assertion: monotonic reads should never have violations
    always(
        monotonic_ok,
        "No monotonic read violations found",
        monotonic_details
    )
    
    print("Client: all tests completed!")