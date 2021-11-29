import { SinglePageStorage } from './single.page.storage'
import { Storage } from './share'
export { Mem, Storage } from './share'

export const NewStorage = (initialSize: number): Storage => {
  return new SinglePageStorage(initialSize)
}

export const LoadStorage = (buffer: Buffer): Storage => {
  return SinglePageStorage.load(buffer)
}
