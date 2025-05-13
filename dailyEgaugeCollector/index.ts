import { spawnLambda, generateDayIntervals, appendCsvToMonthlyFile } from "../shared/utils";

export const handler = async () => {
  const now = new Date();
  now.setDate(now.getDate() - 1); // yesterday
  const intervals = generateDayIntervals(now);

  const results = await Promise.all(
    intervals.map(({ f, n, s }) =>
      spawnLambda("fetchEgaugeData", { f, n, s })
    )
  );

  const allRows = results.flat();
  allRows.sort((a, b) =>
    new Date(a["Date & Time"]).getTime() - new Date(b["Date & Time"]).getTime()
  );

  await appendCsvToMonthlyFile(allRows, now);
};
