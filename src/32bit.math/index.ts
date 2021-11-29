/* eslint-disable @typescript-eslint/no-this-alias */
/* eslint-disable no-extend-native */
export { }

export const MIN_SIGNED_32 = -0x80000000
export const MAX_SIGNED_32 = 0x7FFFFFFF
export const MIN_VALUE_32 = -0xFFFFFFFF
export const MAX_VALUE_32 = 0xFFFFFFFF

// const OPENER = 64n + 1n
// const OPENED = 1n << OPENER

declare global {
  interface Number {
    low32: () => number
    highestOneBit32: () => number
    numberOfLeadingZeros32: () => number
    numberOfTrailingZeros32: () => number
  }
}

const clz32 = (v: number) => {
  // if (v <= 0) {
  //   return v === 0 ? 32 : 0
  // }

  // let n = 31
  // if (v >= 1 << 16) { n -= 16; v >>>= 16 }
  // if (v >= 1 << 8) { n -= 8; v >>>= 8 }
  // if (v >= 1 << 4) { n -= 4; v >>>= 4 }
  // if (v >= 1 << 2) { n -= 2; v >>>= 2 }

  // return n - (v >>> 1)
  return Math.clz32(v)
}

const ctz32 = (i: number) => {
  i = ~i & (i - 1)
  if (i <= 0) return i & 32
  let n = 1
  if (i > 1 << 16) { n += 16; i >>>= 16 }
  if (i > 1 << 8) { n += 8; i >>>= 8 }
  if (i > 1 << 4) { n += 4; i >>>= 4 }
  if (i > 1 << 2) { n += 2; i >>>= 2 }
  return n + (i >>> 1)
}

Number.prototype.low32 = function () {
  let v = (this as unknown) as number

  v = (v >= MIN_VALUE_32 && v <= MAX_VALUE_32) ? v : Number(BigInt(v) & 0xFFFFFFFFn)

  return v
}

Number.prototype.numberOfLeadingZeros32 = function () {
  const v = (this as unknown) as number

  return clz32(v.low32())
}

Number.prototype.numberOfTrailingZeros32 = function () {
  const v = (this as unknown) as number

  return ctz32(v.low32())
}
