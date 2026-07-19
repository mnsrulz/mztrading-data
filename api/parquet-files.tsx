/** @jsxImportSource https://esm.sh/preact */
import { renderToString } from "npm:preact-render-to-string@^6.5.13";
import { Hono } from "https://esm.sh/hono@4.12";
import { handle } from "https://esm.sh/hono@4.12/netlify";
import { CboeOptionsRawSummary } from "../lib/data.ts";

type OptionsSummary = { name: string; optionsAssetUrl: string; dt: string, stocksAssetUrl: string };

const app = new Hono();
const optionsSummary: OptionsSummary[] = CboeOptionsRawSummary;

// --- Layout wrapper ---
const Html = ({ children }: { children: preact.ComponentChildren }) => (
  <html lang="en">
    <head>
      <meta charSet="UTF-8" />
      <meta name="viewport" content="width=device-width, initial-scale=1.0" />
      <title>Options Data</title>
      <link rel="stylesheet" href="https://cdn.jsdelivr.net/gh/oupala/apaxy@main/apaxy/theme/style.css"></link>
    </head>
    <body>{children}</body>
  </html>
);

const Home = () => (
  <div>
    <h1>Home</h1>
    <ul>
      <li key="ohlc"><a href="ohlc">ohlc</a></li>
      <li key="files"><a href="files">files</a></li>
    </ul>
  </div>
)

// --- Pages ---
const App = ({ options }: { options: OptionsSummary[] }) => (
  <div>
    <h1>Options Data</h1>
    <ul>
      <li><a href="../">...</a></li>
      {options.map((item) => (
        <li key={item.dt}>
          <a href={`dt=${item.dt}/`} target="_blank">
            dt={item.dt}
          </a>
        </li>
      ))}
    </ul>
  </div>
);

const FilePage = ({ title, fileName }: { title: string, fileName: string }) => (
  <div>
    <h1>{title}</h1>
    <ul>
      <li><a href="../">...</a></li>
      <li>
        <a href={fileName} target="_blank">
          {fileName}
        </a>
      </li>
    </ul>
  </div>
);

// --- Middlewares ---
app.use(async (c, next) => {
  console.log(`request received: ${c.req.method} ${c.req.path}`);
  await next();
});

// --- API routes ---
app.get("/", (c) => {
  const html =
    "<!DOCTYPE html>" + renderToString(<Html><Home /></Html>);
  return c.html(html);
})

// --- HTML routes ---
app.get("/files", (c) => c.redirect("/files/"));

app.get("/files/", (c) => {
  const html =
    "<!DOCTYPE html>" + renderToString(<Html><App options={optionsSummary} /></Html>);
  return c.html(html);
});

app.get("/files/:dt/*.parquet", async (c) => {
  const dtParam = c.req.param("dt");
  const dtMatch = dtParam.match(/dt=(\d{4}-\d{2}-\d{2})/);
  if (!dtMatch) return c.text("Not found", 404);

  const dt = dtMatch[1];
  const match = optionsSummary.find((k) => k.dt === dt);

  if (match) {
    if (c.req.method === "HEAD") {
      return await fetch(match.optionsAssetUrl, {
        method: "HEAD"
      });
    }
    return c.redirect(match.optionsAssetUrl);
  } else {
    return c.text("Custom 404 Message", 404);
  }
});

app.get("/files/:dt", (c) => c.redirect(`${c.req.path}/`));

app.get("/files/:dt/", (c) => {
  const dtParam = c.req.param("dt");
  const dtMatch = dtParam.match(/dt=(\d{4}-\d{2}-\d{2})/);
  if (!dtMatch) return c.text("Not found", 404);

  const dt = dtMatch[1];
  const match = optionsSummary.find((k) => k.dt === dt);

  if (match) {
    const fileName = new URL(match.optionsAssetUrl).pathname.split("/").pop() || '';
    const html =
      "<!DOCTYPE html>" +
      renderToString(<Html><FilePage title={`Options Data for ${dt}`} fileName={fileName} /></Html>);
    return c.html(html);
  }

  const notFoundHtml = "<!DOCTYPE html>" + renderToString(<Html><body>Not found</body></Html>);
  return c.html(notFoundHtml, 404);
});

// --- HTML routes ---
app.get("/ohlc", (c) => c.redirect("/ohlc/"));

app.get("/ohlc/", (c) => {
  const html =
    "<!DOCTYPE html>" + renderToString(<Html><App options={optionsSummary.filter(k => k.stocksAssetUrl)} /></Html>);
  return c.html(html);
});

app.get("/ohlc/:dt/*.parquet", async (c) => {
  const dtParam = c.req.param("dt");
  const dtMatch = dtParam.match(/dt=(\d{4}-\d{2}-\d{2})/);
  if (!dtMatch) return c.text("Not found", 404);

  const dt = dtMatch[1];
  const match = optionsSummary.find((k) => k.dt === dt);

  if (match && match.stocksAssetUrl) {
    if (c.req.method === "HEAD") {
      return await fetch(match.stocksAssetUrl, {
        method: "HEAD"
      });
    }
    return c.redirect(match.stocksAssetUrl);
  } else {
    return c.text("Custom 404 Message", 404);
  }
});

app.get("/ohlc/:dt", (c) => c.redirect(`${c.req.path}/`));

app.get("/ohlc/:dt/", (c) => {
  const dtParam = c.req.param("dt");
  const dtMatch = dtParam.match(/dt=(\d{4}-\d{2}-\d{2})/);
  if (!dtMatch) return c.text("Not found", 404);

  const dt = dtMatch[1];
  const match = optionsSummary.find((k) => k.dt === dt);

  if (match) {
    const html =
      "<!DOCTYPE html>" +
      renderToString(<Html><FilePage title={`Ohlc Data for ${dt}`} fileName={new URL(match.stocksAssetUrl).pathname.split("/").pop() || ''} /></Html>);
    return c.html(html);
  }

  const notFoundHtml = "<!DOCTYPE html>" + renderToString(<Html><body>Not found</body></Html>);
  return c.html(notFoundHtml, 404);
});

export default handle(app);
export {
  app
}