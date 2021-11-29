export class AssertionError extends Error {

}

export const DLAssertions = {
  notInUse: (p: number) => {
    throw new AssertionError(`Chunk at ${p} is not in use`)
  },
  afterNotInUse: (p: number) => {
    throw new AssertionError(`Chunk after ${p} does not see this chunk as in use`)
  },
  nextIncorrect: (p: number) => {
    throw new AssertionError(`Previous chunk to ${p} is marked free but has an incorrect next pointer`)
  },
  unaligned: (p: number, q: number) => {
    throw new AssertionError(`Chunk address [mem: ${p} => chunk: ${q}] is incorrectly aligned`)
  },
  invalidAddress: (p: number) => {
    throw new AssertionError(`Memory address ${p} is invalid`)
  },
  notAllocated: (mem: number) => {
    throw new AssertionError(`Address ${mem} has not been allocated`)
  },
  problemWithNext: (p: number, next: number, previousInUse: boolean) => {
    throw new AssertionError(`Problem with next chunk [${p}]["${next}: previous-inuse="${previousInUse ? 'true' : 'false'}"]`)
  }
}
