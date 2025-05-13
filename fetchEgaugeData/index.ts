import { parse } from "csv-parse/sync";
import axios from "axios";

export const handler = async (event: {
  f: number;
  n: number;
  s: number;
}): Promise<any[]> => {
  const { f, n, s } = event;
  
  const url = `https://egauge15897.egaug.es/cgi-bin/egauge-show?E&c&S&s=${s}&n=${n}&f=${f}&C&Z=LST8LDT7%2CM3.2.0%2F02%3A00%2CM11.1.0%2F02%3A00`;
  const res = await axios.get(url, { responseType: "text" });

  const records = parse(res.data, {
    columns: true,
    skip_empty_lines: true,
  });

  return records;
};
