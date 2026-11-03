using Workerd = import "/workerd/workerd.capnp";
const config :Workerd.Config = (
  services = [
    (name = "main", worker = .w),
    (name = "do-storage", disk = (path = "do-storage", writable = true)),
  ],
  sockets = [ (name = "http", address = "127.0.0.1:8900", http = (), service = "main") ],
  cluster = (sharedDirectory = "do-storage", network = "unix", key = "alarm-test-only"),
);
const w :Workerd.Worker = (
  modules = [ (name = "worker.js", esModule = embed "worker.js") ],
  compatibilityDate = "2026-09-01",
  durableObjectNamespaces = [ (className = "A", uniqueKey = "alarm-test-A") ],
  durableObjectStorage = (localDisk = "do-storage"),
  bindings = [ (name = "A", durableObjectNamespace = "A"), (name = "NODE", fromEnvironment = "NODE") ],
);
