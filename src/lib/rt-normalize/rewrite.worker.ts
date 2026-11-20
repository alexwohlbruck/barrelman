import { createRewriter, owned, type RewriteJob } from './rewrite'

declare const self: Worker
const rewriter = createRewriter()

type Request = { id: number } & ({ op: 'rewrite'; job: RewriteJob } | { op: 'calls'; body: Uint8Array })

self.onmessage = async ({ data }: MessageEvent<Request>) => {
  try {
    if (data.op === 'rewrite') {
      const feed = await rewriter.rewrite(data.job)
      const body = owned(feed.body)
      self.postMessage({ id: data.id, result: { ...feed, body } }, [body.buffer])
    } else {
      self.postMessage({ id: data.id, result: await rewriter.calls(data.body) })
    }
  } catch (error) {
    self.postMessage({ id: data.id, error: error instanceof Error ? error.message : String(error) })
  }
}
