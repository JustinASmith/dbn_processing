use databento::dbn::{Action, MboMsg};

// This struct stores the aggregated MBO data for a given day
// since the data files contain data from prior days as catchup data
#[derive(Debug)]
pub struct AggregatedData {
    pub messages: Vec<MboMsg>, // Vecotor to hold MboMsgs for the day
    total_volume: u64,         // Total activity volume for the day
    traded_volume: u64,        // Total traded volume for the day
    total_msgs: u64,           // Total messages for the day
}

// Default impl for AggregatedData
impl Default for AggregatedData {
    fn default() -> Self {
        Self::new()
    }
}

// Implement methods for AggregatedData
impl AggregatedData {
    // Create a new AggregatedData struct
    pub fn new() -> Self {
        Self {
            messages: Vec::new(),
            total_volume: 0,
            traded_volume: 0,
            total_msgs: 0,
        }
    }

    // Add a MboMsg to the AggregatedData
    pub fn add_msg(&mut self, msg: MboMsg) -> anyhow::Result<()> {
        let action: Action = msg.action()?; // Propogate error if any
        let size = msg.size as u64;

        if action == Action::Trade {
            self.traded_volume += size;
        }
        self.total_volume += size;
        self.total_msgs += 1;

        self.messages.push(msg); // msg is moved here

        Ok(())
    }

    // Sort the final aggregated data
    pub fn sort(&mut self) {
        self.messages.sort_by(|a, b| {
            a.hd.ts_event
                .cmp(&b.hd.ts_event)
                .then_with(|| a.sequence.cmp(&b.sequence))
        });
    }
}
