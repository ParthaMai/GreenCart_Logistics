
const fs = require('fs');
const csv = require('csv-parser');
const createCsvWriter = require('csv-writer').createObjectCsvWriter;
const path = require('path');

const DRIVERS_CSV = path.join(__dirname, 'drivers.csv');
const ORDERS_CSV = path.join(__dirname, 'orders.csv');
const ROUTES_CSV = path.join(__dirname, 'routes.csv');
const OUTPUT_CSV = path.join(__dirname, 'assignments.csv');

const TRAFFIC_FACTOR = {
  'Low': 1.0,
  'Medium': 1.2,
  'High': 1.5
};

function hourToMinutes(time) {
   if (!time || typeof time !== 'string') return 0;

  const [hours, minutes] = time.split(':');
  if (!hours || !minutes) return 0;

  const h = parseInt(hours, 10) || 0;
  const m = parseInt(minutes, 10) || 0;

  return h * 60 + m;
}

function avgPastWeek(pw) {

  if (!pw || typeof pw !== 'string') {
    return { arr: [], total: 0, avg: 0 };
  }

  const arr = pw.split('|').map(num => Number(num) || 0);

  let total = 0;
  for (let n of arr) {
    total += n;
  }

  let avg = total / arr.length;

  return { arr, total, avg };
}

// read CSV 
function readCsv(filePath) {
  return new Promise((resolve, reject) => {
    const out = [];
    fs.createReadStream(filePath)
      .pipe(csv())
      .on('data', row => out.push(row))
      .on('end', () => resolve(out))
      .on('error', err => reject(err));
  });
}

async function main() {
  try {
    const [driversRaw, ordersRaw, routesRaw] = await Promise.all([
      readCsv(DRIVERS_CSV),
      readCsv(ORDERS_CSV),
      readCsv(ROUTES_CSV)
    ]);

    const routes = {};
    routesRaw.forEach(r => {
      const id = String(r.route_id).trim();
      routes[id] = {
        route_id: id,
        distance_km: parseFloat(r.distance_km) || 0,
        traffic_level: (r.traffic_level || 'Low').trim(),
        base_time_min: parseFloat(r.base_time_min) || 0
      };
    });

    const drivers = driversRaw.map(d => {
      const shift_hours = parseFloat(d.shift_hours) || 0;
      const past = avgPastWeek(d.past_week_hours || '');
      return {
        name: d.name,
        shift_hours,
        past_week_arr: past.arr,
        past_week_total: past.total,
        past_week_avg: past.avg,
        assigned_minutes: 0,
        assignments: []
      };
    });

    const orders = ordersRaw.map(o => {
      const route = routes[String(o.route_id).trim()];
      const delivery_time_min = hourToMinutes(o.delivery_time);
      const base_time_min = route.base_time_min;
      const traffic = route.traffic_level;
      const factor = TRAFFIC_FACTOR[traffic];
      const estimated_time = Math.round(base_time_min * factor);

      const final_time = Math.max(estimated_time, delivery_time_min);

      return {
        order_id: String(o.order_id).trim(),
        value_rs: parseFloat(o.value_rs) || 0,
        route_id: String(o.route_id).trim(),
        delivery_time_min,
        base_time_min,
        traffic_level: traffic,
        total_time: final_time
      };
    });

    // Sort orders by delivery_time
    orders.sort((a, b) => a.delivery_time_min - b.delivery_time_min);

    for (const ord of orders) {
      const ordHours = ord.total_time / 60.0;

      const candidates = drivers.filter(dr => {
        const assignedHoursSoFar = dr.assigned_minutes / 60.0;
        return (assignedHoursSoFar + ordHours) <= dr.shift_hours + 1e-9;
      });

      if (candidates.length === 0) {
        ord.assigned_to = null;
        continue;
      }

      candidates.sort((d1, d2) => {
        const time1 = (d1.past_week_avg) + (d1.assigned_minutes / 60.0);
        const time2 = (d2.past_week_avg) + (d2.assigned_minutes / 60.0);
        if (time1 !== time2)
            return time1 - time2;
        // remaining hours
        const rem1 = d1.shift_hours - (d1.assigned_minutes / 60.0);
        const rem2 = d2.shift_hours - (d2.assigned_minutes / 60.0);
        return rem2 - rem1;
      });

      const chosen = candidates[0];
      // assign_min
      chosen.assigned_minutes += ord.total_time;
      chosen.assignments.push({
        order_id: ord.order_id,
        route_id: ord.route_id,
        total_time: ord.total_time,
        value_rs: ord.value_rs
      });
      ord.assigned_to = chosen.name;
    }

    const outputRows = [];
    orders.forEach(o => {
      outputRows.push({
        order_id: o.order_id,
        route_id: o.route_id,
        value_rs: o.value_rs,
        total_time: o.total_time,
        traffic_level: o.traffic_level,
        assigned_to: o.assigned_to || 'UNASSIGNED'
      });
    });

    const csvWriter = createCsvWriter({
      path: OUTPUT_CSV,
      header: [
        {id: 'order_id', title: 'order_id'},
        {id: 'route_id', title: 'route_id'},
        {id: 'value_rs', title: 'value_rs'},
        {id: 'adjusted_time_min', title: 'adjusted_time_min'},
        {id: 'traffic_level', title: 'traffic_level'},
        {id: 'assigned_to', title: 'assigned_to'}
      ]
    });

    await csvWriter.writeRecords(outputRows);
    console.log('Assignments written to', OUTPUT_CSV);

    console.log('\nDriver summary:');

    drivers.forEach(d => {
      const totalAssignedHours = (d.assigned_minutes / 60.0).toFixed(2);
      console.log(`\nDriver: ${d.name}`);
      console.log(`  Shift hours: ${d.shift_hours}`);
      console.log(`  Past week avg hrs/day: ${d.past_week_avg.toFixed(2)}`);
      console.log(`  Assigned hours today: ${totalAssignedHours}`);
      console.log(`  Orders assigned (${d.assignments.length}):`);
      d.assignments.forEach(a => {
        console.log(`    - Order ${a.order_id} (route ${a.route_id}) - ${a.total_time} min - Rs ${a.value_rs}`);
      });
    });

    // Print unassigned orders
    const unassigned = orders.filter(o => !o.assigned_to);
    if (unassigned.length) {
      console.log('\nUnassigned orders:');
      unassigned.forEach(u => {
        console.log(`  - Order ${u.order_id} (route ${u.route_id}) requires ${u.total_time} min`);
      });
    } else {
      console.log('\nAll orders assigned.');
    }

  } catch (err) {
    console.error('Error:', err);
  }
}

main();