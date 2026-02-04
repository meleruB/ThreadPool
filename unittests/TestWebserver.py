import time
import unittest

from app.routes import webserver

class TestApp(unittest.TestCase):
    def setUp(self):
        webserver.testing = True
        self.app = webserver.test_client()

    def test_states_mean(self):
        payload = {'question': 'Percent of adults aged 18 years and older who have an overweight classification'}
        response = self.app.post('/api/states_mean', json=payload)
        job_id = response.json['job_id']
        self.assertEqual(response.status_code, 200)
        self.assertTrue(int(job_id) > 0)
        nr_of_tries = 10

        while nr_of_tries > 0:
            response = self.app.get(f'/api/get_results/{job_id}', json=payload)
            if response.json['status'] == 'done':
                break
            nr_of_tries -= 1
            time.sleep(0.1)

        self.assertIsNotNone(response.json['data'])
        data = response.json['data']
        self.assertEqual(data['Alabama'], 34.1551724137931)
        self.assertEqual(data['Alaska'], 35.90277777777778)
        self.assertEqual(data['Arizona'], 35.4046875)
        self.assertEqual(data['Arkansas'], 32.99516129032258)
        self.assertEqual(data['California'], 35.72459016393442)

    def test_state_mean(self):
        payload = {'question': 'Percent of adults aged 18 years and older who have an overweight classification', 'state': 'Ohio'}
        response = self.app.post('/api/state_mean', json=payload)
        job_id = response.json['job_id']
        self.assertEqual(response.status_code, 200)
        self.assertTrue(int(job_id) > 0)
        nr_of_tries = 10

        while nr_of_tries > 0:
            response = self.app.get(f'/api/get_results/{job_id}', json=payload)
            if response.json['status'] == 'done':
                break
            nr_of_tries -= 1
            time.sleep(0.1)

        self.assertIsNotNone(response.json['data'])
        data = response.json['data']
        self.assertEqual(data['Ohio'], 33.25753424657535)

    def test_best_5(self):
        payload = {'question': 'Percent of adults aged 18 years and older who have an overweight classification'}
        response = self.app.post('/api/best5', json=payload)
        job_id = response.json['job_id']
        self.assertEqual(response.status_code, 200)
        self.assertTrue(int(job_id) > 0)
        nr_of_tries = 10

        while nr_of_tries > 0:
            response = self.app.get(f'/api/get_results/{job_id}', json=payload)
            if response.json['status'] == 'done':
                break
            nr_of_tries -= 1
            time.sleep(0.1)

        self.assertIsNotNone(response.json['data'])
        data = response.json['data']
        self.assertEqual(data['Arkansas'], 32.99516129032258)
        self.assertEqual(data['District of Columbia'], 30.746875)
        self.assertEqual(data['Kentucky'], 33.071641791044776)
        self.assertEqual(data['Missouri'], 32.76268656716418)
        self.assertEqual(data['Vermont'], 33.118181818181824)

    def test_worst_5(self):
        payload = {'question': 'Percent of adults aged 18 years and older who have an overweight classification'}
        response = self.app.post('/api/worst5', json=payload)
        job_id = response.json['job_id']
        self.assertEqual(response.status_code, 200)
        self.assertTrue(int(job_id) > 0)
        nr_of_tries = 10

        while nr_of_tries > 0:
            response = self.app.get(f'/api/get_results/{job_id}', json=payload)
            if response.json['status'] == 'done':
                break
            nr_of_tries -= 1
            time.sleep(0.1)

        self.assertIsNotNone(response.json['data'])
        data = response.json['data']
        self.assertEqual(data['Alaska'], 35.90277777777778)
        self.assertEqual(data['Montana'], 36.17826086956522)
        self.assertEqual(data['Nevada'], 36.358333333333334)
        self.assertEqual(data['New Jersey'], 36.080597014925374)
        self.assertEqual(data['Puerto Rico'], 36.986363636363635)

    def test_global_mean(self):
        payload = {'question': 'Percent of adults aged 18 years and older who have an overweight classification', 'state': 'Ohio'}
        response = self.app.post('/api/global_mean', json=payload)
        job_id = response.json['job_id']
        self.assertEqual(response.status_code, 200)
        self.assertTrue(int(job_id) > 0)
        nr_of_tries = 10

        while nr_of_tries > 0:
            response = self.app.get(f'/api/get_results/{job_id}', json=payload)
            if response.json['status'] == 'done':
                break
            nr_of_tries -= 1
            time.sleep(0.1)

        self.assertIsNotNone(response.json['data'])
        data = response.json['data']
        self.assertEqual(data['global_mean'], 34.482761415833565)

    def test_diff_from_mean(self):
        payload = {'question': 'Percent of adults aged 18 years and older who have an overweight classification'}
        response = self.app.post('/api/diff_from_mean', json=payload)
        job_id = response.json['job_id']
        self.assertEqual(response.status_code, 200)
        self.assertTrue(int(job_id) > 0)
        nr_of_tries = 10

        while nr_of_tries > 0:
            response = self.app.get(f'/api/get_results/{job_id}', json=payload)
            if response.json['status'] == 'done':
                break
            nr_of_tries -= 1
            time.sleep(0.1)

        self.assertIsNotNone(response.json['data'])
        data = response.json['data']
        self.assertEqual(data['Alabama'], 0.3275890020404617)
        self.assertEqual(data['Alaska'], -1.4200163619442137)
        self.assertEqual(data['Arizona'], -0.9219260841664365)
        self.assertEqual(data['Arkansas'], 1.487600125510987)
        self.assertEqual(data['California'], -1.2418287481008576)

    def test_state_diff_from_mean(self):
        payload = {'question': 'Percent of adults aged 18 years and older who have an overweight classification', 'state': 'Ohio'}
        response = self.app.post('/api/state_diff_from_mean', json=payload)
        job_id = response.json['job_id']
        self.assertEqual(response.status_code, 200)
        self.assertTrue(int(job_id) > 0)
        nr_of_tries = 10

        while nr_of_tries > 0:
            response = self.app.get(f'/api/get_results/{job_id}', json=payload)
            if response.json['status'] == 'done':
                break
            nr_of_tries -= 1
            time.sleep(0.1)

        self.assertIsNotNone(response.json['data'])
        data = response.json['data']
        self.assertEqual(data['Ohio'], 1.2252271692582184)

    def test_mean_by_category(self):
        payload = {'question': 'Percent of adults aged 18 years and older who have an overweight classification'}
        response = self.app.post('/api/mean_by_category', json=payload)
        job_id = response.json['job_id']
        self.assertEqual(response.status_code, 200)
        self.assertTrue(int(job_id) > 0)
        nr_of_tries = 10

        while nr_of_tries > 0:
            response = self.app.get(f'/api/get_results/{job_id}', json=payload)
            if response.json['status'] == 'done':
                break
            nr_of_tries -= 1
            time.sleep(0.1)

        self.assertIsNotNone(response.json['data'])
        data = response.json['data']
        self.assertEqual(data["('Alabama', 'Age (years)', '18 - 24')"], 24.9)
        self.assertEqual(data["('Alabama', 'Age (years)', '25 - 34')"], 33.0)
        self.assertEqual(data["('Kansas', 'Education', 'Some college or technical school')"], 35.5)

    def test_state_mean_by_category(self):
        payload = {'question': 'Percent of adults aged 18 years and older who have an overweight classification', 'state': 'Ohio'}
        response = self.app.post('/api/state_mean_by_category', json=payload)
        job_id = response.json['job_id']
        self.assertEqual(response.status_code, 200)
        self.assertTrue(int(job_id) > 0)
        nr_of_tries = 10

        while nr_of_tries > 0:
            response = self.app.get(f'/api/get_results/{job_id}', json=payload)
            if response.json['status'] == 'done':
                break
            nr_of_tries -= 1
            time.sleep(0.1)

        self.assertIsNotNone(response.json['data'])
        data = response.json['data']['Ohio']
        self.assertEqual(data["('Age (years)', '18 - 24')"], 29.1)
        self.assertEqual(data["('Age (years)', '25 - 34')"], 31.933333333333334)
        self.assertEqual(data["('Age (years)', '35 - 44')"], 33.96666666666667)

if __name__ == "__main__":
    unittest.main()