/** @jsxImportSource https://esm.sh/preact */
import { Hono } from "https://esm.sh/hono@4.12.27";
import { toSSG } from "https://esm.sh/hono@4.12.27/ssg";
import { renderToString } from "npm:preact-render-to-string@^6.5.13";
import { CboeOptionsRawSummary } from "../lib/data.ts";
import fs from "node:fs/promises";

type OptionsSummary = { name: string; optionsAssetUrl: string; dt: string, stocksAssetUrl: string };
const optionsSummary: OptionsSummary[] = CboeOptionsRawSummary;

const app = new Hono();

// --- Components ---
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
      <li><a href="/ohlc/">ohlc</a></li>
      <li><a href="/files/">files</a></li>
    </ul>
  </div>
);

const App = ({ options, prefix }: { options: OptionsSummary[]; prefix: string }) => (
  <div>
    <h1>Options Data</h1>
    <ul>
      <li><a href="/">...</a></li>
      {options.map((item) => (
        <li key={item.dt}>
          <a href={`/${prefix}/dt=${item.dt}/`}>
            dt={item.dt}
          </a>
        </li>
      ))}
    </ul>
  </div>
);

// Changed to accept a direct link URL alongside the friendly display name
const FilePage = ({ title, fileUrl, displayName }: { title: string, fileUrl: string, displayName: string }) => (
  <div>
    <h1>{title}</h1>
    <ul>
      <li><a href="../">...</a></li>
      <li>
        {/* The href now links directly to the remote parquet host */}
        <a href={fileUrl} target="_blank">
          {displayName}
        </a>
      </li>
    </ul>
  </div>
);

// --- Static HTML Routes ---
app.get("/", (c) => c.html("<!DOCTYPE html>" + renderToString(<Html><Home /></Html>)));

app.get("/files/", (c) => c.html("<!DOCTYPE html>" + renderToString(<Html><App options={optionsSummary} prefix="files" /></Html>)));

app.get("/ohlc/", (c) => c.html("<!DOCTYPE html>" + renderToString(<Html><App options={optionsSummary.filter(k => k.stocksAssetUrl)} prefix="ohlc" /></Html>)));

// Dynamic HTML Routes generated strictly per existing data item
optionsSummary.forEach((match) => {
  // 1. Files detail page
  app.get(`/files/dt=${match.dt}/`, (c) => {
    const fileName = new URL(match.optionsAssetUrl).pathname.split("/").pop() || '';
    return c.html("<!DOCTYPE html>" + renderToString(
      <Html>
        <FilePage 
          title={`Options Data for ${match.dt}`} 
          fileUrl={match.optionsAssetUrl} // Direct absolute URL
          displayName={fileName} 
        />
      </Html>
    ));
  });

  // 2. OHLC detail page
  if (match.stocksAssetUrl) {
    app.get(`/ohlc/dt=${match.dt}/`, (c) => {
      const fileName = new URL(match.stocksAssetUrl).pathname.split("/").pop() || '';
      return c.html("<!DOCTYPE html>" + renderToString(
        <Html>
          <FilePage 
            title={`Ohlc Data for ${match.dt}`} 
            fileUrl={match.stocksAssetUrl} // Direct absolute URL
            displayName={fileName} 
          />
        </Html>
      ));
    });
  }
});

// --- Handle compilation ---
if (import.meta.main) {
  // Generate static site files to 'dist' folder
  const res = await toSSG(app, fs, { dir: "dist" });
  if (!res.success) {
    console.error("SSG Build Failed", res.error);
    Deno.exit(1);
  }

  console.log("Static Generation complete! No redirect mapping required.");
}