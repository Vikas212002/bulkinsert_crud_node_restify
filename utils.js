const { v4: uuidv4 } = require('uuid');
const { faker } = require('@faker-js/faker');

async function bulkinsertion() {
    try {
        const bulkdata = [];
        const agents = [
            'John', 'Ramesh', 'Suresh', 'Rohan', 'Mohan',
            'Sohan', 'Rohit', 'Mohit', 'Rahul', 'Raj'
        ];

        const campaigns = [
            'Airtel', 'VI', 'JIO', 'NoBroker', 'Campaign5'
        ];

        const process = [
            'Sales', 'Collection', 'Credit Card', 'Loan', 'FD'
        ];

        let disposeType, disposeName, hold, mute, ringing, transfer, conference, call, duration;
        for (let i = 0; i < 10; i++) {

            let reportType = ['dispose', 'missed', 'autofailed', 'autodrop'][Math.floor(Math.random() * 4)];

            if (reportType === 'dispose') {
                disposeType = ['call back', 'dnc', 'etx'][Math.floor(Math.random() * 3)];
                disposeName = ['External', 'Transfer', 'Do not call', 'Follow up'][Math.floor(Math.random() * 4)];
                hold = Math.floor(Math.random() * 300000);
                mute = Math.floor(Math.random() * 300000);
                ringing = Math.floor(Math.random() * 300000);
                transfer = Math.floor(Math.random() * 300000);
                conference = Math.floor(Math.random() * 300000);
                call = Math.floor(Math.random() * 300000);
                duration = hold + mute + ringing + transfer + conference + call;
            } else {
                disposeType = 'NULL';
                disposeName = ['External', 'Transfer'][Math.floor(Math.random() * 2)];
                hold = 0;
                mute = 0;
                ringing = Math.floor(Math.random() * 300000);
                transfer = 0;
                conference = 0;
                call = 0;
                duration = 0;
            }

            const data = {
                date_time: faker.date.past(),
                type: reportType,
                dispose_type: disposeType,
                dispose_name: disposeName,
                duration: duration,
                agent_name: agents[Math.floor(Math.random() * agents.length)],
                campaign_name: campaigns[Math.floor(Math.random() * campaigns.length)],
                process_name: process[Math.floor(Math.random() * process.length)],
                leadset: uuidv4(),
                reference_uuid: uuidv4(),
                customer_uuid: uuidv4(),
                hold: hold,
                mute: mute,
                ringing: ringing,
                transfer: transfer,
                conference: conference,
                callkey: call,
                dispose_time: Math.floor(Math.random() * 300000),
            };
            bulkdata.push(data);
        }

        
    } catch (err) {
        console.log(err);

    }
}

module.exports = bulkinsertion;