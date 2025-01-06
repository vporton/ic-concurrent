/// Execute several fibers concurently,
/// with a limit on the number of fibers running at the same time, not to overflow an IC queue.
///
/// WARNING: Untested, may not work as expected.
import Timer "mo:base/Timer";

module {
  public type Pid = Nat;

  public type ConcurrentExecutorCanister = actor {
    /// `f` should call `fiberCleanup` when it is done.
    fiberExecute: (f: shared (pid: Pid, data: Blob) -> async ()) -> async ();
  };
  
  public class ConcurrentExecutor(executor: ConcurrentExecutorCanister, maxFibers: Nat, pauseTime: Timer.Duration) {
    var nextPid = 0;

    var fibersCount = 0;

    /// If this function is called several times in a row, their `executor.add` calls are likely
    /// to fall into the same time. This seems to be not a problem, because IC will probably optimize
    /// these short calls to execute in the same block.
    public func add<system>(f: shared (pid: Pid, data: Blob) -> async (), data: Blob): async* ()/*Pid*/ {
      fibersCount += 1;
      if (fibersCount > maxFibers) {
        ignore Timer.setTimer<system>(pauseTime, func (): async () {
          await executor.fiberExecute(f);
        });
      } else {
        let pid = nextPid;
        nextPid += 1;
        await f(pid, data);
      };
      // pid;
    };

    /// Callback to be called by `f`, when it is done.
    public func cleanup(_pid: Pid) {
      fibersCount -= 1;
    };
  };
};
