import { app } from './api/api.ts'

Deno.serve(app.fetch);