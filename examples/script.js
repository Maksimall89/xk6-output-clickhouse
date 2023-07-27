import http from 'k6/http';
import { check, group } from "k6";

export const options = {
    scenarios: {
        contacts: {
            executor: 'ramping-vus',
            startVUs: 0,
            stages: [
                { duration: '20s', target: 10 },
                { duration: '20s', target: 15 },
                { duration: '30s', target: 15 },
                { duration: '10s', target: 0 },
            ],
            gracefulRampDown: '0s',
        },
    },
};

export default function () {
    http.get('https://test.k6.io/news.php');

    // fail check
    let res = http.post("http://test.k6.io/contacts.php");
    check(res, {
        "status code is 500": (r) => r.status === 500
    });

    group("test-group", function() {
        let res = http.get("http://test.k6.io/my_messages.php");
        check(res, {
            "status code is 200": (r) => r.status === 200
        });
    });
};