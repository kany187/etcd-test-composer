#!/usr/bin/env -S python3 -u

# This file serves as a parallel driver for testing monotonic reads.
# It validates that version numbers never decrease across multiple reads from different etcd nodes.

# Antithesis SDK
from antithesis.assertions import (
    always,
    sometimes,
    reachable,
)

import sys
sys.path.append("/opt/antithesis/resources")
import helper


def test_monotonic_reads():
    """
        This function tests the monotonic reads property of etcd.
        
        Strategy:
        1. Write some keys to establish versions
        2. Read each key multiple times from different etcd nodes
        3. Track version numbers and verify they never decrease
        4. Assert that monotonic reads property holds
    """
    
    print("Client: starting monotonic reads test")
    
    # Step 1: Write some test data
    client = helper.connect_to_host()
    num_keys = (helper.generate_requests() % 10) + 5  # Between 5 and 15 keys
    test_keys = []
    
    for _ in range(num_keys):
        key = f"monotonic_{helper.generate_random_string()}"
        value = helper.generate_random_string()
        success, error = helper.put_request(client, key, value)
        
        # Antithesis Assertion: sometimes put requests are successful. A failed request is OK since we expect them to happen.
        sometimes(success, "Client can make successful put requests", {"error": error})
        
        if success:
            test_keys.append(key)
            print(f"Client: successful put with key '{key}'")
        else:
            print(f"Client: unsuccessful put with key '{key}', and error '{error}'")
    
    print(f"Client: wrote {len(test_keys)} keys for testing")
    
    # Step 2: Test monotonic reads on these keys
    highest_version_seen = {}
    total_reads = 0
    violations = 0
    
    for key in test_keys:
        highest_version_seen[key] = -1
        
        print(f"Client: testing key '{key}'")
        
        # Read each key 5 times from potentially different nodes
        for attempt in range(5):
            client = helper.connect_to_host()
            current_version = helper.get_key_version(client, key)
            
            if current_version is not None:
                last_version = highest_version_seen[key]
                total_reads += 1
                
                print(f"Client: attempt {attempt+1} - version={current_version}, last_version={last_version}")
                
                # Check for monotonic property violation
                went_backwards = current_version < last_version
                
                if went_backwards:
                    violations += 1
                    print(f"Client: monotonic read violation - version went from {last_version} to {current_version}")
                
                # Antithesis Assertion: versions must never decrease
                always(
                    not went_backwards,
                    "Version numbers must never decrease (monotonic reads)",
                    {
                        "key": key,
                        "current_version": current_version,
                        "previous_version": last_version,
                        "attempt": attempt + 1
                    }
                )
                
                # Update tracking
                highest_version_seen[key] = max(highest_version_seen[key], current_version)
    
    # Verify test execution
    reachable("Monotonic reads test completed", {"total_reads": total_reads})
    
    # Antithesis Assertion: sometimes monotonic read checks are successful
    sometimes(
        total_reads >= 10,
        "Successfully performed at least 10 monotonic read checks",
        {"total_reads": total_reads, "violations": violations}
    )
    
    # Report results
    success = (violations == 0)
    if success:
        print(f"Client: monotonic reads test passed - {total_reads} reads, 0 violations")
    else:
        print(f"Client: monotonic reads test failed - {violations} violations found")
    
    # Final assertion
    always(
        success,
        "No monotonic read violations found",
        {
            "total_reads": total_reads,
            "violations": violations,
            "keys_tested": len(test_keys)
        }
    )


if __name__ == "__main__":
    test_monotonic_reads()
    print("Client: all tests completed")