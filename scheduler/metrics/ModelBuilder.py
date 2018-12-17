from sklearn.linear_model import LinearRegression
from sklearn.datasets import make_regression

class ModelBuilder:
	def __init__(self):
		self.mapping = {
			"energy": {
				"eiger": 0,
				"moleson": 1,
				"cervino": 2,
				"vully": 3
			},
			"performance": {
				"eiger": 4,
				"moleson": 5,
				"cervino": 6,
				"vully": 7,
			}
		}
        
		self.keys = [[100, 2]]
		self.values = [[0.0,0.01,0.64,0.93,18.7,12.3,14.2,1.9]]
		
		self.model = LinearRegression()
		self.model.fit(self.keys, self.values)
		
	def predict(self, value):
            prediction = self.model.predict(value)
            result = {"energy": {}, "performance": {}}
            for measurement in self.mapping.keys():
                for machine in self.mapping[measurement].keys():
                    result[measurement][machine] = prediction[0][self.mapping[measurement][machine]]
            return result

mb = ModelBuilder()
print(mb.predict([[100,2]]))
