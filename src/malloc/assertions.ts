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
  },
  allocatedTooSmall: (p: number) => {
    throw new AssertionError(`Allocated chunk ${p} is too small`)
  },
  missaligned: (sz: number, p: number) => {
    throw new AssertionError(`Chunk size ${sz} of ${p} is not correctly aligned`)
  },
  allocatedSmallerThanReq: (p: number, sz: number, s: number) => {
    throw new AssertionError(`Allocated chunk ${p} is smaller than requested [${sz} < ${s}]`)
  },
  allocatedTooLarge: (p: number, sz: number, s: number) => {
    throw new AssertionError(`Allocated chunk ${p} is too large (should have been split off) [${sz}  > ${s}]`)
  },
  topChunkInvalid: (p: number) => {
    throw new AssertionError(`Memory address ${p} of top chunk is invalid`)
  },
  topChunkWrong: (sz: number, topSize: number) => {
    throw new AssertionError(`Marked size top chunk ${sz} is not equals to the recorded top size ${topSize}`)
  },
  topChunkNeg: (sz: number) => {
    throw new AssertionError(`Top chunk size ${sz} is not positive`)
  },
  topChunkNotMerged: () => {
    throw new AssertionError('Chunk before top chunk is free - why has it not been merged in to the top chunk?')
  },
  invalidChainLinks: (p: number) => {
    throw new AssertionError(`Free chunk ${p} has invalid chain links`)
  },
  afterNotMerged: (p: number) => {
    throw new AssertionError(`Chunk after free chunk ${p} is free - should have been merged`)
  },
  beforeNotMerged: (p: number) => {
    throw new AssertionError(`Chunk before free chunk ${p} is free - should have been merged`)
  },
  nextIncorrectPreviousSize: (p: number) => {
    throw new AssertionError(`Next chunk after ${p} has an incorrect previous size`)
  },
  userPointerUnaligned: (p: number) => {
    throw new AssertionError(`User pointer for chunk ${p} is not correctly aligned`)
  },
  freeNotMarkedAsFree: (p: number) => {
    throw new AssertionError(`Free chunk ${p} is not marked as free`)
  },
  nextMarkedInUse: (p: number) => {
    throw new AssertionError(`Next chunk after ${p} has it marked as in use`)
  },
  freeTooSmall: (p: number) => {
    throw new AssertionError(`Free chunk ${p} is too small`)
  }
}
