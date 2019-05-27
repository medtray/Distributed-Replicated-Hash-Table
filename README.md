# Distributed-Replicated-Hash-Table
Distributed Replicated Hash Table (RHT)

The RHT offers three APIs: put(K,V)/get(K)/put(<K1,V1>,<K2,V2>,<K3,V3>). The put(K,V) API is responsible for storing the value V associated with the key K. The get(K) API is responsible for retrieving the value associated with the key K. The put(<K1,V1>,<K2,V2>,<K3,V3>) stores the three keys K1,K2,K3 in the RHT atomically.

Two-phase commit protocol(2PC) is used for consistent read and write operations.
