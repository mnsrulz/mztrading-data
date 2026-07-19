import { app } from './api/parquet-files.tsx'

Deno.serve(app.fetch);