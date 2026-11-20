[ Go Service (cgo) ]                          [ Rust Engine (libfluerd.so) ]

       |                                                    |
1. fluerd_init()  -----------------------------------> Buat Runtime + Channel + Spawn Consumer

       |  <---- *mut EngineHandle (Opaque Pointer) -------- |
       |                                                    |
2. fluerd_push_frame(handle, *const u8, len) --------> slice::from_raw_parts (unsafe)

       |  (Go hold memory selama call berlangsung)          |--> parse_raw_bytes (zero-copy filter)
       |                                                    |--> tx.try_send() / blocking_send()
       |  <---- status code (0 = OK, -1 = Drop/Full) ------ |
       |                                                    |
3. fluerd_shutdown(handle) --------------------------> drop(tx) -> runtime.block_on(consumer)

       |  <---- total_anomaly_score (u64) ----------------- |--> Box::from_raw(handle) (Free memory)
