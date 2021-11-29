import '@/32bit.math'
import { logg } from '@/log'

describe('Test 32 bit math', () => {
  const one = 1
  const max = one << 31
  const abs = Math.abs(max)

  logg(`Bit mask testing of ${abs} => ${abs.toString(2)} (${abs.toString(2).length})`)

  it('Test in Safe Range (One)', () => {
    for (let i = 0; i < 31; i++) {
      const n = one << i

      expect(n.numberOfTrailingZeros32()).toBe(i)
      expect(n.numberOfLeadingZeros32()).toBe(31 - i)
    }
  })

  it('Test in Safe Range (Max)', () => {
    for (let i = 0; i < 31; i++) {
      const n = max >>> i
      expect(n.numberOfLeadingZeros32()).toBe(i)
      expect(n.numberOfTrailingZeros32()).toBe(31 - i)
    }
  })
})
